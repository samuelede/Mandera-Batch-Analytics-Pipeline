"""
data_generator.py
-----------------
Main entry point for Mandera Analytics synthetic data generation.

This script:
1. Generates a unique batch_id for the current run.
2. Calls individual Faker modules for customers, products, and orders.
3. Pushes all generated records into MongoDB Atlas.
4. Prints a generation summary for observability.

Usage:
    python generator/data_generator.py
"""

import os
import uuid
import logging
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from faker_customers import generate_customers
from faker_products import generate_products
from faker_orders import generate_orders

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

MONGO_URI    = os.getenv("MONGO_URI")
MONGO_DB     = os.getenv("MONGO_DB_NAME", "mandera_db")
BATCH_SIZE   = int(os.getenv("BATCH_SIZE", 500))

# Derived counts — orders are the most numerous entity
CUSTOMER_COUNT = max(50,  BATCH_SIZE // 5)
PRODUCT_COUNT  = max(20,  BATCH_SIZE // 10)
ORDER_COUNT    = BATCH_SIZE


# ---------------------------------------------------------------------------
# MongoDB helpers
# ---------------------------------------------------------------------------
def get_mongo_client() -> MongoClient:
    if not MONGO_URI:
        raise EnvironmentError(
            "MONGO_URI is not set. Check your .env file."
        )
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10_000)
    # Verify connection
    client.admin.command("ping")
    log.info("MongoDB Atlas connection established.")
    return client


def insert_records(collection, records: list[dict], label: str) -> int:
    """Insert records and return the count actually written."""
    if not records:
        log.warning("No %s records to insert.", label)
        return 0
    try:
        result = collection.insert_many(records, ordered=False)
        count = len(result.inserted_ids)
        log.info("Inserted %d %s records.", count, label)
        return count
    except BulkWriteError as exc:
        written = exc.details.get("nInserted", 0)
        log.warning(
            "Partial insert for %s: %d written. Details: %s",
            label, written, exc.details,
        )
        return written


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def run_generator() -> dict:
    batch_id   = str(uuid.uuid4())
    batch_time = datetime.now(timezone.utc).isoformat()

    log.info("=" * 60)
    log.info("MANDERA BATCH GENERATOR")
    log.info("Batch ID   : %s", batch_id)
    log.info("Batch Time : %s", batch_time)
    log.info("=" * 60)

    # --- Generate records ---
    log.info("Generating %d customers ...", CUSTOMER_COUNT)
    customers = generate_customers(batch_id, count=CUSTOMER_COUNT)

    log.info("Generating %d products ...", PRODUCT_COUNT)
    products  = generate_products(batch_id, count=PRODUCT_COUNT)

    customer_ids = [c["customer_id"] for c in customers]
    product_ids  = [p["product_id"]  for p in products]

    log.info("Generating %d orders ...", ORDER_COUNT)
    orders = generate_orders(
        batch_id, customer_ids, product_ids, count=ORDER_COUNT
    )

    # --- Push to MongoDB ---
    client = get_mongo_client()
    db     = client[MONGO_DB]

    c_count = insert_records(db["raw_customers"], customers, "customers")
    p_count = insert_records(db["raw_products"],  products,  "products")
    o_count = insert_records(db["raw_orders"],    orders,    "orders")

    client.close()

    summary = {
        "batch_id":        batch_id,
        "batch_time":      batch_time,
        "customers_saved": c_count,
        "products_saved":  p_count,
        "orders_saved":    o_count,
        "total_records":   c_count + p_count + o_count,
    }

    log.info("-" * 60)
    log.info("GENERATION SUMMARY")
    for key, val in summary.items():
        log.info("  %-22s: %s", key, val)
    log.info("=" * 60)

    return summary


if __name__ == "__main__":
    run_generator()
