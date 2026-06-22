"""
transform_orders.py
--------------------
Reads raw.orders_raw (settings.RAW_TABLES["orders"]) from
PostgreSQL, applies cleaning and standardisation, and writes to
staging.orders_clean (settings.STAGING_TABLES["orders"]).

Table names are imported from config/settings.py — never hardcoded.

Transformations applied:
- Drop rows where order_id, customer_id, or product_id is null
  (catches the missing_product_rate bad records).
- Drop rows where total_amount is null or <= 0
  (catches invalid_amount_rate and negative_amount_rate bad records).
- Drop rows with order_status outside the accepted domain
  (catches invalid_status_rate bad records).
- Normalise order_status and payment_method to lowercase.
- Cast order_date to TIMESTAMP.
- Compute net_amount = total_amount - discount_amount.
- Add pipeline metadata columns (transformed_at).

Usage:
    python transformation/transform_orders.py --batch-id <batch_id>
"""

import sys
import os
import logging
import argparse
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "config"))
from settings import POSTGRES_URL, RAW_TABLES, STAGING_TABLES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

RAW_TABLE     = RAW_TABLES["orders"]      # raw.orders_raw
STAGING_TABLE = STAGING_TABLES["orders"]  # staging.orders_clean

VALID_ORDER_STATUSES = {"completed", "pending", "cancelled", "refunded"}


def transform_orders(batch_id: str) -> dict:
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
        log.warning("No raw orders found for batch %s", batch_id)
        engine.dispose()
        return {"batch_id": batch_id, "raw": 0, "staged": 0, "dropped": 0}

    # --- Transformations ---
    df = df.dropna(subset=["order_id", "customer_id", "product_id"])

    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
    df = df[df["total_amount"].notna() & (df["total_amount"] > 0)]

    df["order_status"]   = df["order_status"].str.lower().str.strip()
    df["payment_method"] = df["payment_method"].str.lower().str.strip()
    df["region"]         = df["region"].str.strip().str.title()

    df = df[df["order_status"].isin(VALID_ORDER_STATUSES)]

    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce", utc=True)

    df["net_amount"] = (df["total_amount"] - df["discount_amount"].fillna(0)).round(2)

    df["transformed_at"] = datetime.now(timezone.utc).isoformat()

    staged_count = len(df)
    dropped = raw_count - staged_count
    log.info("After transformations: %d records | Dropped: %d", staged_count, dropped)

    schema, table = STAGING_TABLE.split(".")
    df.to_sql(name=table, con=engine, schema=schema, if_exists="append", index=False, method="multi")
    log.info("Wrote %d records to %s", staged_count, STAGING_TABLE)

    engine.dispose()
    return {"batch_id": batch_id, "raw": raw_count, "staged": staged_count, "dropped": dropped}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    result = transform_orders(args.batch_id)
    log.info("Transform result: %s", result)