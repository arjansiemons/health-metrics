import json
import os
import sys
import logging
import time
import uuid
from datetime import datetime
from functools import wraps

from fastapi import FastAPI, HTTPException, Request
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from geolib import geohash
from contextlib import contextmanager

BATCH_SIZE = 1000

# Enhanced logging setup
logger = logging.getLogger("console-output")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s - [%(request_id)s] - %(levelname)s - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Add request_id to log context
class RequestIdFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'request_id'):
            record.request_id = 'no-request-id'
        return True

logger.addFilter(RequestIdFilter())

app = FastAPI()

# Database configuration
data_store = os.environ.get("DATA_STORE", None)
db_host = os.environ.get("DB_HOST", "health-db-rw.postgresql-system.svc.cluster.local")
db_port = os.environ.get("DB_PORT", "5432")
db_name = os.environ.get("DB_NAME", "healthdb")
db_user = os.environ.get("DB_USER", "postgres")
db_password = os.environ.get("DB_PASSWORD", "postgres")

DATABASE_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(DATABASE_URL)

def log_timing(func):
    """Decorator to log function execution time"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"{func.__name__} completed in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"{func.__name__} failed after {execution_time:.2f} seconds")
            raise
    return wrapper

@contextmanager
def get_db_connection():
    """Context manager for database connections with enhanced logging"""
    try:
        connection = engine.connect()
        logger.debug("Database connection established")
        yield connection
    except SQLAlchemyError as e:
        logger.error(f"Database connection error: {str(e)}")
        raise
    finally:
        connection.close()
        logger.debug("Database connection closed")

@log_timing
def write_metrics_batch(connection, metrics_data):
    """Write a batch of metrics data to the database with enhanced logging"""
    try:
        batch_size = len(metrics_data)
        logger.debug(f"Writing metrics batch of size {batch_size}")

        metrics_insert = """
            INSERT INTO metrics (metric_name, timestamp, value, field_name)
            VALUES (:metric_name, :timestamp, :value, :field_name)
            RETURNING id
        """

        tags_insert = """
            INSERT INTO metrics_tags (metric_id, key, value)
            VALUES (:metric_id, :key, :value)
        """

        for item in metrics_data:
            try:
                result = connection.execute(
                    text(metrics_insert),
                    {
                        "metric_name": item["measurement"],
                        "timestamp": item["time"],
                        "value": item["fields"]["value"],
                        "field_name": item["field_name"],
                    },
                )
                metric_id = result.scalar()

                for key, value in item["tags"].items():
                    connection.execute(
                        text(tags_insert),
                        {"metric_id": metric_id, "key": key, "value": value}
                    )
            except SQLAlchemyError as e:
                logger.error(f"Error writing metric {item['measurement']}: {str(e)}")
                raise

        logger.debug(f"Successfully wrote {batch_size} metrics")

    except Exception as e:
        logger.error(f"Batch write failed: {str(e)}")
        raise

@log_timing
def write_workouts_batch(connection, workouts_data):
    """Write a batch of workout data to the database with enhanced logging"""
    try:
        batch_size = len(workouts_data)
        logger.debug(f"Writing workouts batch of size {batch_size}")

        workout_insert = """
            INSERT INTO workouts (workout_id, timestamp, lat, lng, geohash)
            VALUES (:workout_id, :timestamp, :lat, :lng, :geohash)
            ON CONFLICT (workout_id, timestamp) DO NOTHING
        """

        for workout in workouts_data:
            try:
                connection.execute(text(workout_insert), workout)
            except SQLAlchemyError as e:
                logger.error(f"Error writing workout {workout['workout_id']}: {str(e)}")
                raise

        logger.debug(f"Successfully wrote {batch_size} workout points")

    except Exception as e:
        logger.error(f"Batch write failed: {str(e)}")
        raise

def split_fields(datapoint: dict):
    """Split fields into data and tags with validation logging"""
    try:
        data = {}
        tags = {}

        if "date" not in datapoint:
            logger.warning("Datapoint missing required 'date' field")

        for field_key, value in datapoint.items():
            if field_key == "date":
                continue

            if isinstance(value, (int, float)):
                data[field_key] = float(value)
            else:
                tags[field_key] = str(value)

        return data, tags
    except Exception as e:
        logger.error(f"Error splitting fields: {str(e)}")
        raise

@log_timing
def ingest_workouts(workouts: list):
    """Ingest workout data into PostgreSQL with enhanced logging"""
    logger.info(f"Starting workout ingestion for {len(workouts)} workouts")
    transformed_workout_data = []
    total_points = 0

    try:
        for workout in workouts:
            workout_id = f"{workout['name']}-{workout['start']}-{workout['end']}"
            points_count = len(workout["route"])
            logger.debug(f"Processing workout {workout_id} with {points_count} points")

            for gps_point in workout["route"]:
                point = {
                    "workout_id": workout_id,
                    "timestamp": gps_point["timestamp"],
                    "lat": gps_point["lat"],
                    "lng": gps_point["lon"],
                    "geohash": geohash.encode(gps_point["lat"], gps_point["lon"], 7),
                }
                transformed_workout_data.append(point)
                total_points += 1

                if len(transformed_workout_data) >= BATCH_SIZE:
                    with get_db_connection() as conn:
                        write_workouts_batch(conn, transformed_workout_data)
                    transformed_workout_data = []

        if transformed_workout_data:
            with get_db_connection() as conn:
                write_workouts_batch(conn, transformed_workout_data)

        logger.info(f"Workout ingestion complete. Total points processed: {total_points}")

    except Exception as e:
        logger.error(f"Workout ingestion failed: {str(e)}")
        raise

@log_timing
def ingest_metrics(metrics: list):
    """Ingest metrics data into PostgreSQL with enhanced logging"""
    logger.info(f"Starting metrics ingestion for {len(metrics)} metric types")
    transformed_data = []
    total_datapoints = 0

    try:
        for metric in metrics:
            metric_name = metric["name"]
            points_count = len(metric["data"])
            logger.debug(f"Processing metric {metric_name} with {points_count} datapoints")

            for datapoint in metric["data"]:
                data, tags = split_fields(datapoint)

                for field_name, value in data.items():
                    point = {
                        "measurement": metric_name,
                        "time": datapoint["date"],
                        "tags": tags,
                        "fields": {"value": value},
                        "field_name": field_name,
                    }
                    transformed_data.append(point)
                    total_datapoints += 1

                    if len(transformed_data) >= BATCH_SIZE:
                        with get_db_connection() as conn:
                            write_metrics_batch(conn, transformed_data)
                        transformed_data = []

        if transformed_data:
            with get_db_connection() as conn:
                write_metrics_batch(conn, transformed_data)

        logger.info(f"Metrics ingestion complete. Total datapoints processed: {total_datapoints}")

    except Exception as e:
        logger.error(f"Metrics ingestion failed: {str(e)}")
        raise

@app.middleware("http")
async def add_request_id(request: Request, call_next):
    """Middleware to add request ID to each request"""
    request_id = str(uuid.uuid4())
    logger.addFilter(RequestIdFilter())
    logging.LoggerAdapter(logger, {"request_id": request_id})
    
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    
    logger.info(f"Request processed in {process_time:.2f} seconds")
    return response

@app.get("/")
async def root():
    logger.debug("Root endpoint called")
    return {"message": "Health Metrics API"}

@app.post("/")
async def collect(healthkit_data: dict):
    logger.info("Starting data collection request")
    
    if data_store is not None:
        filename = os.path.join(data_store, f"{datetime.now().isoformat()}.json")
        try:
            with open(filename, "w") as f:
                f.write(json.dumps(healthkit_data))
            logger.debug(f"Raw data saved to {filename}")
        except IOError as e:
            logger.error(f"Failed to save raw data: {str(e)}")

    try:
        ingest_metrics(healthkit_data.get("data", {}).get("metrics", []))
        ingest_workouts(healthkit_data.get("data", {}).get("workouts", []))
    except SQLAlchemyError as e:
        logger.exception("Database error occurred")
        logger.error(f"Database error details: {str(e)}")
        raise HTTPException(status_code=500, detail="Database Error")
    except Exception as e:
        logger.exception("Unexpected error occurred")
        logger.error(f"Error details: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

    return "Ok"

@app.get("/health")
async def health():
    logger.debug("Health check endpoint called")
    try:
        with get_db_connection() as conn:
            conn.execute(text("SELECT 1"))
        logger.debug("Health check successful")
        return "Ok"
    except SQLAlchemyError as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail="Database Unavailable")

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting Health Metrics API server")
    uvicorn.run(app, host="0.0.0.0", port=7788, reload=True)
