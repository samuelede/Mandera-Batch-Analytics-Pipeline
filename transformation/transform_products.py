"""
transform_products.py
---------------------
Reads raw.products_raw (settings.RAW_TABLES["products"]) from
PostgreSQL, applies cleaning and standardisation, and writes to
staging.products_clean (settings.STAGING_TABLES["products"]).

Table names are imported from config/settings.py — never hardcoded.

IDEMPOTENCY: before inserting, any existing rows for the same
batch_id are deleted from staging.products_clean.

Required transformations implemented (in order):
1. Duplicate removal   — drop rows with a duplicate product_id
                          within the batch (keeps the first occurrence).
2. Null replacement    — product_id is the only field that, if
                          missing, makes a record unsalvageable and is
                          dropped. Every other null is replaced:
                            product_name -> 'Unnamed Product'
                            sku          -> 'UNKNOWN-SKU'
                            category     -> 'Uncategorized'
                            cost_price   -> 60% of unit_price
                            is_active    -> True
                          unit_price <= 0 cannot be repaired with a
                          placeholder without distorting downstream
                          financial totals, so those rows are dropped.
3. Type enforcement    — unit_price/cost_price enforced as numeric,
                          is_active cast to BOOLEAN.
4. Standardized naming — category title-cased, sku upper-cased,
                          product_name stripped/title-cased.
5. Derived field        — margin_pct, price_band.

CATEGORY MISMATCH HANDLING: the generator's category_mismatch_rate
reassigns a product's category to an unrelated (but still valid)
value — e.g. a "Wireless Earbuds" product labeled "Groceries". This
cannot be detected by a null check (the field is present and looks
legitimate) and cannot be silently "corrected" without knowing the
true category, which the generator discards. Rather than pass this
through unflagged or guess at a fix, every row is checked against a
PRODUCT_CATEGORIES lookup (product_name -> expected category) and
flagged via category_verified = FALSE when they disagree. The
category value itself is left as-is; category_verified tells
downstream consumers the field is unreliable for that row.

legacy_discount_code (schema-drift field) is dropped before staging.

Usage:
    python transformation/transform_products.py --batch-id <batch_id>
"""

import sys
import os
import logging
import argparse
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "config"))
from settings import POSTGRES_URL, RAW_TABLES, STAGING_TABLES, PRODUCT_CATEGORIES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

RAW_TABLE     = RAW_TABLES["products"]      # raw.products_raw
STAGING_TABLE = STAGING_TABLES["products"]  # staging.products_clean

# Build a reverse lookup: product_name -> the category it actually
# belongs to, per settings.PRODUCT_CATEGORIES. Used to detect
# category_mismatch_rate injected bad records.
PRODUCT_NAME_TO_CATEGORY = {
    name: category
    for category, names in PRODUCT_CATEGORIES.items()
    for name in names
}


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


def price_band(price: float) -> str:
    if pd.isna(price):
        return "unknown"
    if price < 50:
        return "low"
    if price < 200:
        return "mid"
    return "high"


def check_category_match(row) -> bool:
    """
    Return True if category matches the expected category for this
    product_name per PRODUCT_CATEGORIES, or if product_name isn't in
    the lookup at all (can't verify, so don't penalize it).
    """
    expected = PRODUCT_NAME_TO_CATEGORY.get(row["product_name"])
    if expected is None:
        return True  # unrecognised name (e.g. already replaced with
                      # 'Unnamed Product') — nothing to verify against
    return row["category"] == expected


def transform_products(batch_id: str) -> dict:
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
        log.warning("No raw products found for batch %s", batch_id)
        engine.dispose()
        return {"batch_id": batch_id, "raw": 0, "staged": 0, "dropped": 0, "duplicates_removed": 0}

    # ------------------------------------------------------------------
    # 1. DUPLICATE REMOVAL
    # ------------------------------------------------------------------
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["product_id"], keep="first")
    duplicates_removed = before_dedup - len(df)
    if duplicates_removed:
        log.info("Removed %d duplicate product_id row(s).", duplicates_removed)

    # Unsalvageable: no product_id, or a non-positive price.
    df = df.dropna(subset=["product_id"])
    df = df[df["unit_price"] > 0]

    # ------------------------------------------------------------------
    # CATEGORY MISMATCH CHECK — done BEFORE category is null-replaced,
    # so we're checking the original raw value against product_name.
    # ------------------------------------------------------------------
    df["category_verified"] = df.apply(check_category_match, axis=1)
    mismatch_count = (~df["category_verified"]).sum()
    if mismatch_count:
        log.info(
            "Flagged %d row(s) where category does not match product_name "
            "(category_verified=False). Value left as-is — true category "
            "is not recoverable from the source data.",
            mismatch_count,
        )

    # ------------------------------------------------------------------
    # 2. NULL REPLACEMENT (record is kept, value is filled)
    # ------------------------------------------------------------------
    df["product_name"] = df["product_name"].replace("", None).fillna("Unnamed Product")
    df["sku"] = df["sku"].replace("", None).fillna("UNKNOWN-SKU")
    df["category"] = df["category"].replace("", None).fillna("Uncategorized")
    df["cost_price"] = df["cost_price"].fillna((df["unit_price"] * 0.60).round(2))
    df["is_active"] = df["is_active"].fillna(True)

    # ------------------------------------------------------------------
    # 3. TYPE ENFORCEMENT
    # ------------------------------------------------------------------
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["cost_price"] = pd.to_numeric(df["cost_price"], errors="coerce")
    df["is_active"] = df["is_active"].astype(bool)

    # ------------------------------------------------------------------
    # 4. STANDARDIZED NAMING
    # ------------------------------------------------------------------
    df["category"] = df["category"].str.strip().str.title()
    df["sku"] = df["sku"].str.strip().str.upper()
    df["product_name"] = df["product_name"].str.strip().str.title()

    if "legacy_discount_code" in df.columns:
        df = df.drop(columns=["legacy_discount_code"])

    # ------------------------------------------------------------------
    # 5. SIMPLE DERIVED FIELDS
    # ------------------------------------------------------------------
    df["margin_pct"] = (
        (df["unit_price"] - df["cost_price"]) / df["unit_price"]
    ).round(4)
    df["price_band"] = df["unit_price"].apply(price_band)

    df["transformed_at"] = datetime.now(timezone.utc).isoformat()

    staged_count = len(df)
    dropped = raw_count - staged_count
    log.info(
        "After transformations: %d records | Dropped (unsalvageable): %d | "
        "Duplicates removed: %d | Category mismatches flagged: %d",
        staged_count, dropped, duplicates_removed, mismatch_count,
    )

    # --- Idempotency guard — clear any prior rows for this batch first ---
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
        "category_mismatches_flagged": int(mismatch_count),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    result = transform_products(args.batch_id)
    log.info("Transform result: %s", result)