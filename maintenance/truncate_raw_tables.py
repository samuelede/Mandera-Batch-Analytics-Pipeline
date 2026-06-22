"""
truncate_raw_tables.py
----------------------
Truncates PostgreSQL raw schema tables after a successful staging load.

Table names are imported from config/settings.py RAW_TABLES /
STAGING_TABLES — never hardcoded.

REQUIRED LOGIC: transform complete -> truncate raw tables.
Truncation must NOT happen if transformation failed for ANY entity.

Safety check performed, per entity, before truncation:
    For each of customers/products/orders independently:
      - If raw has 0 rows for this batch, that entity is trivially
        "complete" (nothing to transform) — does not block truncation.
      - If raw has > 0 rows but staging has EXACTLY 0 rows for this
        batch, that is treated as transformation having failed or
        never having run for that entity — truncation is ABORTED
        for ALL THREE raw tables, not just the failing one. This is
        deliberate: truncating customers_raw while products_raw's
        transform failed would still destroy data while leaving the
        pipeline in a known-bad partial state.

This is a stricter check than "is staging non-empty anywhere in the
batch" (which an earlier version of this script used) — that OR-style
check could pass even when one entity's transform crashed entirely,
as long as a DIFFERENT entity's transform succeeded. A per-entity AND
check is required to satisfy "truncation must not happen if
transformation fails."

This intentionally does NOT compare exact row counts (raw vs staging
counts legitimately differ due to deliberate data quality filtering —
see docs/data_dictionary.md "Data Quality Simulation"). It only checks
presence vs absence, which is what distinguishes "transform ran and
produced output" from "transform did not run or crashed before writing
anything."

Usage:
    python maintenance/truncate_raw_tables.py --batch-id <batch_id>
    python maintenance/truncate_raw_tables.py --batch-id <batch_id> --force
"""

import sys
import os
import logging
import argparse

from sqlalchemy import create_engine, text

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "config"))
from settings import POSTGRES_URL, RAW_TABLES, STAGING_TABLES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def count_rows(engine, table_full: str, batch_id: str) -> int:
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT COUNT(*) FROM {table_full} WHERE batch_id = :bid"),
            {"bid": batch_id},
        )
        return result.scalar()


def verify_transformation_complete(engine, batch_id: str) -> dict:
    """
    Check, per entity, whether transformation can be considered
    complete for this batch:
        - raw_count == 0  -> trivially complete (nothing to transform)
        - raw_count  > 0 AND staging_count == 0 -> INCOMPLETE
          (transform did not run, or ran and produced zero output —
           either way, truncating raw now would destroy the only
           copy of data that was never successfully transformed)
        - raw_count  > 0 AND staging_count  > 0 -> complete

    Returns a dict of per-entity results plus an overall boolean.
    """
    results = {}
    all_complete = True

    for entity in RAW_TABLES:
        raw_count = count_rows(engine, RAW_TABLES[entity], batch_id)
        staging_count = count_rows(engine, STAGING_TABLES[entity], batch_id)

        if raw_count == 0:
            complete = True
            reason = "no raw rows for this batch — nothing to transform"
        elif staging_count > 0:
            complete = True
            reason = f"raw={raw_count}, staging={staging_count}"
        else:
            complete = False
            reason = f"raw={raw_count}, staging=0 — transform did not produce output"
            all_complete = False

        results[entity] = {
            "raw_count": raw_count,
            "staging_count": staging_count,
            "complete": complete,
            "reason": reason,
        }

        log.info(
            "[%s] %s — %s",
            "OK" if complete else "INCOMPLETE", entity, reason,
        )

    return {"entities": results, "all_complete": all_complete}


def truncate_raw_tables(batch_id: str, force: bool = False) -> dict:
    engine = create_engine(POSTGRES_URL, echo=False)

    if not force:
        log.info("Verifying transformation completed for every entity before truncating ...")
        verification = verify_transformation_complete(engine, batch_id)

        if not verification["all_complete"]:
            incomplete = [
                entity for entity, r in verification["entities"].items()
                if not r["complete"]
            ]
            msg = (
                f"Truncation ABORTED for batch {batch_id}. Transformation did not "
                f"complete for: {incomplete}. No raw tables were truncated — this "
                f"includes entities whose transform DID succeed, since truncating "
                f"only some raw tables would leave the pipeline in a partial, "
                f"inconsistent state. Re-run the failed transform(s) and try again, "
                f"or use --force to override (not recommended outside debugging)."
            )
            log.error(msg)
            engine.dispose()
            raise RuntimeError(msg)

        log.info("Transformation verified complete for all entities. Proceeding with truncation.")
    else:
        log.warning("--force flag set — skipping transformation-completeness check.")

    truncated = []

    with engine.begin() as conn:
        for table_full in RAW_TABLES.values():
            conn.execute(text(f"TRUNCATE TABLE {table_full}"))
            log.info("Truncated: %s", table_full)
            truncated.append(table_full)

    engine.dispose()
    log.info("Raw table truncation complete for batch %s. Tables cleared: %s", batch_id, truncated)
    return {"batch_id": batch_id, "truncated": truncated}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--force", action="store_true",
                        help="Skip transformation-completeness check and truncate unconditionally")
    args = parser.parse_args()
    truncate_raw_tables(args.batch_id, force=args.force)