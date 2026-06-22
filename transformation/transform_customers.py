"""
transform_customers.py
----------------------
Reads raw.customers_raw (settings.RAW_TABLES["customers"]) from
PostgreSQL, applies cleaning and standardisation, and writes to
staging.customers_clean (settings.STAGING_TABLES["customers"]).

Table names are imported from config/settings.py — never hardcoded.

IDEMPOTENCY: before inserting, any existing rows for the same
batch_id are deleted from staging.customers_clean.

Required transformations implemented (in order):
1. Duplicate removal   — drop rows with a duplicate customer_id
                          within the batch (keeps the first occurrence).
2. Null replacement    — customer_id is the only field that, if
                          missing, makes a record unsalvageable and is
                          dropped. Every other null/invalid field is
                          replaced:
                            email (missing OR invalid format)
                                              -> unique per-row
                                                 placeholder (see below)
                            city              -> 'Unknown'
                            segment           -> 'unknown'
                            is_active         -> False
3. Type enforcement    — registration_date cast to DATE,
                          is_active cast to BOOLEAN.
4. Standardized naming — email lowercased + stripped, city/segment
                          stripped and title-cased / lowercased.
5. Derived field        — full_name, customer_tenure_days.

EMAIL PLACEHOLDER UNIQUENESS: a basic regex catches both missing AND
invalid-format emails (e.g. "invalid_email@@"), which a missing-value
check alone would not catch. Each replaced email gets a placeholder
keyed to that row's own customer_id (e.g.
"unknown-a1b2c3d4@unknown.com") rather than one shared constant
string. Using a single shared placeholder for every bad email would
make unrelated customers look like duplicates of each other under
the duplicate_email check in validate_data_quality.py — a genuine
side effect discovered after this fix shipped, not a hypothetical.

Usage:
    python transformation/transform_customers.py --batch-id <batch_id>
"""

import sys
import os
import re
import logging
import argparse
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "config"))
from settings import POSTGRES_URL, RAW_TABLES, STAGING_TABLES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

RAW_TABLE     = RAW_TABLES["customers"]      # raw.customers_raw
STAGING_TABLE = STAGING_TABLES["customers"]  # staging.customers_clean

EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def clear_existing_batch(engine, table_full: str, batch_id: str) -> int:
    """Delete any existing rows for this batch_id before re-inserting."""
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


def is_valid_email(value) -> bool:
    if pd.isna(value) or not isinstance(value, str) or value.strip() == "":
        return False
    return bool(EMAIL_PATTERN.match(value.strip()))


def placeholder_email(customer_id: str) -> str:
    """
    Build a placeholder email unique to this row, using the first 8
    characters of customer_id, so two different customers with bad
    emails never collide into an apparent duplicate.
    """
    short_id = str(customer_id)[:8] if pd.notna(customer_id) else "00000000"
    return f"unknown-{short_id}@unknown.com"


def transform_customers(batch_id: str) -> dict:
    engine = create_engine(POSTGRES_URL, echo=False)

    log.info("Reading %s for batch %s ...", RAW_TABLE, batch_id)
    df = pd.read_sql(
        f"SELECT * FROM {RAW_TABLE} WHERE batch_id = %(batch_id)s",
        con=engine,
        params={"batch_id": batch_id},
    )

    raw_count = len(df)
    log.info("Raw record count: %d", raw_count)

    if df.empty:
        log.warning("No raw customers found for batch %s", batch_id)
        engine.dispose()
        return {"batch_id": batch_id, "raw": 0, "staged": 0, "dropped": 0, "duplicates_removed": 0}

    # ------------------------------------------------------------------
    # 1. DUPLICATE REMOVAL
    # ------------------------------------------------------------------
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["customer_id"], keep="first")
    duplicates_removed = before_dedup - len(df)
    if duplicates_removed:
        log.info("Removed %d duplicate customer_id row(s).", duplicates_removed)

    df = df.dropna(subset=["customer_id"])

    # ------------------------------------------------------------------
    # 2. NULL REPLACEMENT (record is kept, value is filled)
    # ------------------------------------------------------------------
    invalid_email_mask = ~df["email"].apply(is_valid_email)
    invalid_email_count = invalid_email_mask.sum()
    df.loc[invalid_email_mask, "email"] = df.loc[invalid_email_mask, "customer_id"].apply(
        placeholder_email
    )
    if invalid_email_count:
        log.info(
            "Replaced %d missing/invalid-format email value(s) with a "
            "per-row unique placeholder (not a shared constant) to "
            "avoid false duplicate_email collisions.",
            invalid_email_count,
        )

    df["city"] = df["city"].replace("", None).fillna("Unknown")
    df["segment"] = df["segment"].replace("", None).fillna("unknown")
    df["is_active"] = df["is_active"].fillna(False)

    # ------------------------------------------------------------------
    # 3. TYPE ENFORCEMENT
    # ------------------------------------------------------------------
    df["registration_date"] = pd.to_datetime(
        df["registration_date"], errors="coerce"
    ).dt.date
    df["is_active"] = df["is_active"].astype(bool)

    # ------------------------------------------------------------------
    # 4. STANDARDIZED NAMING
    # ------------------------------------------------------------------
    df["email"] = df["email"].str.lower().str.strip()
    df["city"] = df["city"].str.strip().str.title()
    df["segment"] = df["segment"].str.strip().str.lower()
    df["first_name"] = df["first_name"].str.strip().str.title()
    df["last_name"] = df["last_name"].str.strip().str.title()

    # ------------------------------------------------------------------
    # 5. SIMPLE DERIVED FIELDS
    # ------------------------------------------------------------------
    df["full_name"] = df["first_name"].fillna("") + " " + df["last_name"].fillna("")
    df["full_name"] = df["full_name"].str.strip()

    today = datetime.now(timezone.utc).date()
    df["customer_tenure_days"] = df["registration_date"].apply(
        lambda d: (today - d).days if pd.notna(d) else None
    )

    df["transformed_at"] = datetime.now(timezone.utc).isoformat()

    staged_count = len(df)
    dropped = raw_count - staged_count
    log.info(
        "After transformations: %d records | Dropped (unsalvageable): %d | Duplicates removed: %d | "
        "Invalid/missing emails replaced: %d",
        staged_count, dropped, duplicates_removed, invalid_email_count,
    )

    clear_existing_batch(engine, STAGING_TABLE, batch_id)

    schema, table = STAGING_TABLE.split(".")
    df.to_sql(name=table, con=engine, schema=schema, if_exists="append", index=False, method="multi")
    log.info("Wrote %d records to %s", staged_count, STAGING_TABLE)

    engine.dispose()
    return {
        "batch_id": batch_id,
        "raw": raw_count,
        "staged": staged_count,
        "dropped": dropped,
        "duplicates_removed": duplicates_removed,
        "invalid_emails_replaced": int(invalid_email_count),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    result = transform_customers(args.batch_id)
    log.info("Transform result: %s", result)