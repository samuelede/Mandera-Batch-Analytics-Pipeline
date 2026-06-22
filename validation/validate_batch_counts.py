"""
validate_batch_counts.py
------------------------
Compares record counts across MongoDB, PostgreSQL raw tables, and
PostgreSQL staging tables for a given batch_id.

Table names are imported from config/settings.py RAW_TABLES /
STAGING_TABLES — never hardcoded. MongoDB collection names come
from settings.MONGO_COLLECTIONS.

Writes results to monitoring.batch_row_counts in PostgreSQL.
Flags variance between raw and staging exceeding threshold.

Usage:
    python validation/validate_batch_counts.py --batch-id <batch_id>
"""

import sys
import os
import logging
import argparse
from datetime import datetime, timezone

from pymongo import MongoClient
from sqlalchemy import create_engine, text

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "config"))
from settings import (
    MONGO_URI, MONGO_DB, MONGO_COLLECTIONS,
    POSTGRES_URL, RAW_TABLES, STAGING_TABLES,
    validate_mongo_config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

VARIANCE_THRESHOLD = 0.05


def count_mongo(db, collection: str, batch_id: str) -> int:
    return db[collection].count_documents({"batch_id": batch_id})


def count_pg_table(engine, table_full: str, batch_id: str) -> int:
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_full} WHERE batch_id = :bid"),
            {"bid": batch_id},
        )
        return result.scalar()


def write_monitoring_record(engine, record: dict) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO monitoring.batch_row_counts (
                    batch_id, entity, mongo_count, raw_count,
                    staging_count, variance_pct, status, recorded_at
                ) VALUES (
                    :batch_id, :entity, :mongo_count, :raw_count,
                    :staging_count, :variance_pct, :status, :recorded_at
                )
            """),
            record,
        )


def validate_batch(batch_id: str) -> list[dict]:
    validate_mongo_config()
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    mongo_client.admin.command("ping")
    db = mongo_client[MONGO_DB]

    engine = create_engine(POSTGRES_URL, echo=False)

    results = []

    for entity, collection in MONGO_COLLECTIONS.items():
        mongo_count   = count_mongo(db, collection, batch_id)
        raw_count     = count_pg_table(engine, RAW_TABLES[entity],     batch_id)
        staging_count = count_pg_table(engine, STAGING_TABLES[entity], batch_id)

        variance_pct = (
            round((raw_count - staging_count) / raw_count, 4) if raw_count > 0 else 0.0
        )
        status = "OK" if variance_pct <= VARIANCE_THRESHOLD else "WARN"

        record = {
            "batch_id":      batch_id,
            "entity":        entity,
            "mongo_count":   mongo_count,
            "raw_count":     raw_count,
            "staging_count": staging_count,
            "variance_pct":  variance_pct,
            "status":        status,
            "recorded_at":   datetime.now(timezone.utc).isoformat(),
        }

        write_monitoring_record(engine, record)

        log.info(
            "[%s] %s | MongoDB: %d | Raw: %d | Staging: %d | Variance: %.1f%%",
            status, entity, mongo_count, raw_count, staging_count, variance_pct * 100,
        )

        if status == "WARN":
            log.warning(
                "Variance threshold exceeded for %s: %.1f%% > %.1f%%",
                entity, variance_pct * 100, VARIANCE_THRESHOLD * 100,
            )

        results.append(record)

    mongo_client.close()
    engine.dispose()
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    validate_batch(args.batch_id)