"""
extract_mongo_to_minio.py
-------------------------
Extracts records from MongoDB Atlas raw collections and writes them
to MinIO object storage as Parquet files.

NOTE: MinIO partitioning is unaffected by the raw/staging table
renaming — this script only reads from MongoDB collections
(settings.MONGO_COLLECTIONS), which are separate from the
PostgreSQL RAW_TABLES/STAGING_TABLES naming.

Partition structure:
    s3://<bucket>/<entity>/year=YYYY/month=MM/day=DD/batch=<batch_id>/<entity>.parquet

Usage:
    python extraction/extract_mongo_to_minio.py --batch-id <batch_id>
    python extraction/extract_mongo_to_minio.py          # uses latest batch
"""

import sys
import os
import io
import logging
import argparse
from datetime import datetime, timezone

import pandas as pd
from pymongo import MongoClient
from minio import Minio

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "config"))
from settings import (
    MONGO_URI, MONGO_DB, MONGO_COLLECTIONS,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET,
    validate_mongo_config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def get_mongo_client() -> MongoClient:
    validate_mongo_config()
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    client.admin.command("ping")
    return client


def get_minio_client() -> Minio:
    secure = MINIO_ENDPOINT.startswith("https://")
    endpoint = MINIO_ENDPOINT.replace("https://", "").replace("http://", "")
    return Minio(endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=secure)


def ensure_bucket(minio_client: Minio, bucket: str) -> None:
    if not minio_client.bucket_exists(bucket):
        minio_client.make_bucket(bucket)
        log.info("Created MinIO bucket: %s", bucket)
    else:
        log.info("MinIO bucket exists: %s", bucket)


def get_latest_batch_id(db, collection_name: str) -> str | None:
    doc = db[collection_name].find_one({}, {"batch_id": 1}, sort=[("_id", -1)])
    return doc["batch_id"] if doc else None


def build_object_key(entity: str, batch_id: str, now: datetime) -> str:
    return (
        f"{entity}/"
        f"year={now.year:04d}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"batch={batch_id}/"
        f"{entity}.parquet"
    )


def extract_collection_to_minio(db, minio_client, collection_name, batch_id, now) -> int:
    log.info("Extracting collection: %s | batch: %s", collection_name, batch_id)

    cursor = db[collection_name].find({"batch_id": batch_id}, {"_id": 0})
    records = list(cursor)

    if not records:
        log.warning("No records found in %s for batch %s", collection_name, batch_id)
        return 0

    df = pd.DataFrame(records)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    entity = collection_name  # MONGO_COLLECTIONS keys/values are identical entity names
    object_key = build_object_key(entity, batch_id, now)
    size = buffer.getbuffer().nbytes

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_key,
        data=buffer,
        length=size,
        content_type="application/octet-stream",
    )

    log.info("Uploaded %d records → s3://%s/%s (%.1f KB)",
             len(records), MINIO_BUCKET, object_key, size / 1024)
    return len(records)


def run_extraction(batch_id: str | None = None) -> dict:
    now = datetime.now(timezone.utc)
    client = get_mongo_client()
    db = client[MONGO_DB]

    if batch_id is None:
        batch_id = get_latest_batch_id(db, MONGO_COLLECTIONS["orders"])
        if not batch_id:
            raise RuntimeError("No batch_id found in MongoDB. Run the generator first.")
        log.info("No batch_id supplied — using latest: %s", batch_id)

    minio_client = get_minio_client()
    ensure_bucket(minio_client, MINIO_BUCKET)

    summary = {"batch_id": batch_id, "extracted": {}}

    for entity, collection in MONGO_COLLECTIONS.items():
        count = extract_collection_to_minio(db, minio_client, collection, batch_id, now)
        summary["extracted"][entity] = count

    client.close()
    log.info("Extraction to MinIO complete: %s", summary)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract MongoDB → MinIO")
    parser.add_argument("--batch-id", type=str, default=None)
    args = parser.parse_args()
    run_extraction(batch_id=args.batch_id)