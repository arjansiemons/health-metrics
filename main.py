import json
import os
import sys
import logging
from datetime import datetime

from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from geolib import geohash
from contextlib import contextmanager

BATCH_SIZE = 1000

logger = logging.getLogger("console-output")
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

app = FastAPI()

# Database configuration
data_store = os.environ.get("DATA_STORE", None)
db_host = os.environ.get("DB_HOST", "health-db-rw.postgresql-system.svc.cluster.local")
db_port = os.environ.get("DB_PORT", "5432")
db_name = os.environ.get("DB_NAME", "health")
db_user = os.environ.get("DB_USER", "postgres")
db_password = os.environ.get("DB_PASSWORD", "postgres")

DATABASE_URL = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
engine = create_engine(DATABASE_URL)


@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    with engine.connect() as connection:
        yield connection


def write_metrics_batch(connection, metrics_data):
    """Write a batch of metrics data to the database"""
    # Insert metrics
    metrics_insert = """
        INSERT INTO metrics (metric_name, timestamp, value, field_name)
        VALUES (:metric_name, :timestamp, :value, :field_name)
        RETURNING id
    """

    # Insert tags
    tags_insert = """
        INSERT INTO metrics_tags (metric_id, key, value)
        VALUES (:metric_id, :key, :value)
    """

    for item in metrics_data:
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

        # Insert associated tags
        for key, value in item["tags"].items():
            connection.execute(
                text(tags_insert), {"metric_id": metric_id, "key": key, "value": value}
            )


def write_workouts_batch(connection, workouts_data):
    """Write a batch of workout data to the database"""
    workout_insert = """
        INSERT INTO workouts (workout_id, timestamp, lat, lng, geohash)
        VALUES (:workout_id, :timestamp, :lat, :lng, :geohash)
        ON CONFLICT (workout_id, timestamp) DO NOTHING
    """

    for workout in workouts_data:
        connection.execute(text(workout_insert), workout)


def split_fields(datapoint: dict):
    """Split fields into data and tags"""
    data = {}
    tags = {}

    for field_key, value in datapoint.items():
        if field_key == "date":
            continue

        if isinstance(value, (int, float)):
            data[field_key] = float(value)
        else:
            tags[field_key] = str(value)

    return data, tags


def ingest_workouts(workouts: list):
    """Ingest workout data into PostgreSQL"""
    logger.info("Ingesting Workouts Routes")
    transformed_workout_data = []

    for workout in workouts:
        workout_id = f"{workout['name']}-{workout['start']}-{workout['end']}"

        for gps_point in workout["route"]:
            point = {
                "workout_id": workout_id,
                "timestamp": gps_point["timestamp"],
                "lat": gps_point["lat"],
                "lng": gps_point["lon"],
                "geohash": geohash.encode(gps_point["lat"], gps_point["lon"], 7),
            }
            transformed_workout_data.append(point)

            if len(transformed_workout_data) >= BATCH_SIZE:
                with get_db_connection() as conn:
                    write_workouts_batch(conn, transformed_workout_data)
                transformed_workout_data = []

    # Write any remaining data
    if transformed_workout_data:
        with get_db_connection() as conn:
            write_workouts_batch(conn, transformed_workout_data)

    logger.info("Ingesting Workouts Complete")


def ingest_metrics(metrics: list):
    """Ingest metrics data into PostgreSQL"""
    logger.info("Ingesting Metrics")
    transformed_data = []

    for metric in metrics:
        for datapoint in metric["data"]:
            data, tags = split_fields(datapoint)

            for field_name, value in data.items():
                point = {
                    "measurement": metric["name"],
                    "time": datapoint["date"],
                    "tags": tags,
                    "fields": {"value": value},
                    "field_name": field_name,
                }
                transformed_data.append(point)

                if len(transformed_data) >= BATCH_SIZE:
                    with get_db_connection() as conn:
                        write_metrics_batch(conn, transformed_data)
                    transformed_data = []

    # Write any remaining data
    if transformed_data:
        with get_db_connection() as conn:
            write_metrics_batch(conn, transformed_data)

    logger.info("Data Ingestion Complete")


@app.post("/")
async def collect(healthkit_data: dict):
    logger.info("Request received")

    if data_store is not None:
        with open(
            os.path.join(data_store, f"{datetime.now().isoformat()}.json"), "w"
        ) as f:
            f.write(json.dumps(healthkit_data))

    try:
        ingest_metrics(healthkit_data.get("data", {}).get("metrics", []))
        ingest_workouts(healthkit_data.get("data", {}).get("workouts", []))
    except SQLAlchemyError as e:
        logger.exception("Database error occurred")
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Database Error")
    except Exception as e:
        logger.exception("Caught Exception. See stacktrace for details.")
        logger.exception(e)
        raise HTTPException(status_code=500, detail="Internal Server Error")

    return "Ok"


@app.get("/health")
async def health():
    try:
        with get_db_connection() as conn:
            conn.execute(text("SELECT 1"))
        return "Ok"
    except SQLAlchemyError:
        raise HTTPException(status_code=503, detail="Database Unavailable")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=7788, reload=True)
