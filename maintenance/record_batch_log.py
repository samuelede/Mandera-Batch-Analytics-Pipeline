"""
record_batch_log.py
--------------------
Calculates and records one row per entity (customers, products,
orders) in monitoring.batch_log for a given run_id, matching:

    run_id | table_name | batch_date | source_rows | loaded_rows | variance | load_time

Where, per entity:
    source_rows = document count in the MongoDB collection for this run_id
    loaded_rows = row count in the corresponding raw.*_raw table for this run_id
    variance    = source_rows - loaded_rows

Unlike validate_batch_counts.py (which tracks a three-stage
MongoDB/raw/staging percentage variance per entity), this script
writes a simpler two-stage row-count log, one row per table per run,
matching the monitoring.batch_log schema exactly.

Usage:
    python maintenance/record_batch_log.py --run-id <batch_id>
    python maintenance/record_batch_log.py   # uses latest batch
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
    POSTGRES_URL, RAW_TABLES,
    validate_mongo_config,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def get_latest_batch_id(db, collection_name: str) -> str | None:
    doc = db[collection_name].find_one({}, {"batch_id": 1}, sort=[("_id", -1)])
    return doc["batch_id"] if doc else None


def count_source_rows(db, collection: str, run_id: str) -> int:
    return db[collection].count_documents({"batch_id": run_id})


def count_loaded_rows(engine, table_full: str, run_id: str) -> int:
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_full} WHERE batch_id = :rid"),
            {"rid": run_id},
        )
        return result.scalar()


def record_batch_log(run_id: str) -> list[dict]:
    validate_mongo_config()
    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    mongo_client.admin.command("ping")
    db = mongo_client[MONGO_DB]

    engine = create_engine(POSTGRES_URL, echo=False)

    rows_written = []

    for entity, collection in MONGO_COLLECTIONS.items():
        table_full = RAW_TABLES[entity]

        source_rows = count_source_rows(db, collection, run_id)
        loaded_rows = count_loaded_rows(engine, table_full, run_id)
        variance = source_rows - loaded_rows

        log.info(
            "[%s] source=%d loaded=%d variance=%d",
            table_full, source_rows, loaded_rows, variance,
        )

        if variance != 0:
            log.warning(
                "Non-zero variance for %s: %d row(s) present in MongoDB "
                "but missing from the raw table.",
                table_full, variance,
            )

        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO monitoring.batch_log
                        (run_id, table_name, batch_date, source_rows, loaded_rows, variance, load_time)
                    VALUES
                        (:run_id, :table_name, CURRENT_DATE, :source_rows, :loaded_rows, :variance, :load_time)
                """),
                {
                    "run_id": run_id,
                    "table_name": table_full,
                    "source_rows": source_rows,
                    "loaded_rows": loaded_rows,
                    "variance": variance,
                    "load_time": datetime.now(timezone.utc),
                },
            )

        rows_written.append({
            "run_id": run_id,
            "table_name": table_full,
            "source_rows": source_rows,
            "loaded_rows": loaded_rows,
            "variance": variance,
        })

    mongo_client.close()
    engine.dispose()

    log.info("Recorded %d row(s) in monitoring.batch_log for run_id %s", len(rows_written), run_id)
    return rows_written


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-id", type=str, default=None)
    args = parser.parse_args()

    run_id = args.run_id
    if run_id is None:
        validate_mongo_config()
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
        _db = _client[MONGO_DB]
        run_id = get_latest_batch_id(_db, MONGO_COLLECTIONS["orders"])
        _client.close()
        if not run_id:
            raise RuntimeError("No batch_id found in MongoDB. Run the generator first.")
        log.info("No run_id supplied — using latest: %s", run_id)

    record_batch_log(run_id)