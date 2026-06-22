"""
extract_mongo_to_postgres.py
----------------------------
Extracts records from MongoDB Atlas raw collections and loads them
into PostgreSQL raw schema landing tables.

Table names are sourced from config/settings.py RAW_TABLES — never
hardcoded here. If you rename a raw table, update RAW_TABLES (and
the matching SQL in sql/create_raw_tables.sql) and this script
picks up the change automatically.

IDEMPOTENCY: before inserting, any existing rows for the same
batch_id are deleted from the target table. This makes re-running
extraction for the same batch safe (e.g. after an Airflow task
retry) instead of raising a primary key violation on
(customer_id, batch_id) / (product_id, batch_id) / (order_id, batch_id).
A delete-then-insert was chosen over ON CONFLICT DO NOTHING so that
a re-run is visible in loaded_at timestamps, rather than silently
appearing identical to a first run.

Usage:
    python extraction/extract_mongo_to_postgres.py --batch-id <batch_id>
    python extraction/extract_mongo_to_postgres.py   # uses latest batch
"""

import sys
import os
import logging
import argparse
from datetime import datetime, timezone

import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "config"))
from settings import (
    MONGO_URI, MONGO_DB, MONGO_COLLECTIONS,
    POSTGRES_URL, RAW_TABLES,
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


def get_latest_batch_id(db, collection_name: str) -> str | None:
    doc = db[collection_name].find_one({}, {"batch_id": 1}, sort=[("_id", -1)])
    return doc["batch_id"] if doc else None


def clear_existing_batch(engine, table_full: str, batch_id: str) -> int:
    """
    Delete any existing rows for this batch_id from the target table
    before inserting fresh ones. Makes extraction idempotent — safe
    to re-run for the same batch without a primary key violation.

    Returns the number of rows deleted (0 on a genuine first run).
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(f"DELETE FROM {table_full} WHERE batch_id = :bid"),
            {"bid": batch_id},
        )
        deleted = result.rowcount
        if deleted:
            log.info(
                "Cleared %d existing row(s) for batch %s from %s before re-insert.",
                deleted, batch_id, table_full,
            )
        return deleted


def load_to_postgres(engine, df: pd.DataFrame, table_full: str, batch_id: str) -> int:
    """Load a DataFrame into a PostgreSQL raw table given as 'schema.table'."""
    schema, table = table_full.split(".")

    # Idempotency guard — clear any prior rows for this batch first.
    clear_existing_batch(engine, table_full, batch_id)

    df["loaded_at"] = datetime.now(timezone.utc).isoformat()

    df.to_sql(
        name=table,
        con=engine,
        schema=schema,
        if_exists="append",
        index=False,
        method="multi",
    )

    count = len(df)
    log.info("Loaded %d records → %s", count, table_full)
    return count


def extract_collection_to_postgres(db, engine, collection_name, table_full, batch_id) -> int:
    log.info("Extracting %s → %s | batch: %s", collection_name, table_full, batch_id)

    cursor = db[collection_name].find({"batch_id": batch_id}, {"_id": 0})
    records = list(cursor)

    if not records:
        log.warning("No records found in %s for batch %s", collection_name, batch_id)
        return 0

    df = pd.DataFrame(records)
    return load_to_postgres(engine, df, table_full, batch_id)


def run_extraction(batch_id: str | None = None) -> dict:
    mongo_client = get_mongo_client()
    db = mongo_client[MONGO_DB]

    if batch_id is None:
        batch_id = get_latest_batch_id(db, MONGO_COLLECTIONS["orders"])
        if not batch_id:
            raise RuntimeError("No batch_id found in MongoDB. Run the generator first.")
        log.info("No batch_id supplied — using latest: %s", batch_id)

    engine = create_engine(POSTGRES_URL, echo=False)
    summary = {"batch_id": batch_id, "loaded": {}}

    for entity, collection in MONGO_COLLECTIONS.items():
        table_full = RAW_TABLES[entity]
        count = extract_collection_to_postgres(db, engine, collection, table_full, batch_id)
        summary["loaded"][entity] = count

    mongo_client.close()
    engine.dispose()

    log.info("Extraction to PostgreSQL complete: %s", summary)
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract MongoDB → PostgreSQL")
    parser.add_argument("--batch-id", type=str, default=None)
    args = parser.parse_args()
    run_extraction(batch_id=args.batch_id)