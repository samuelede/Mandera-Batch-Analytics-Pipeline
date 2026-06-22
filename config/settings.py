"""
Centralized configuration for the Mandera Analytics pipeline.
All modules import from here — no hardcoded connection strings.

NOTE: This module intentionally does NOT raise on missing environment
variables at import time. Constants like PRODUCT_CATEGORIES, REGIONS,
and table-name mappings are pure config with no external dependency —
they should be importable (e.g. for tests, or by faker_products.py
run standalone) without requiring a live MongoDB Atlas connection.

Call validate_mongo_config() explicitly from any entry point that
actually connects to MongoDB (data_generator.py, extraction scripts).
"""

import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ── MongoDB Atlas ──────────────────────────────────────────────
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")


def validate_mongo_config() -> None:
    """
    Raise if MONGO_URI is missing. Call this explicitly from any
    script that actually opens a MongoDB connection — do not rely
    on import-time validation, since many modules import settings.py
    only for constants (PRODUCT_CATEGORIES, table names, etc.).
    """
    if not MONGO_URI:
        raise EnvironmentError(
            "MONGO_URI is not set. Provide your MongoDB Atlas "
            "connection string in .env"
        )


MONGO_COLLECTIONS = {
    "customers": "raw_customers",
    "products": "raw_products",
    "orders": "raw_orders",
}


# ── Source generation settings (used by generator/) ────────────
ORDERS_MIN = 2000
ORDERS_MAX = 5000
CUSTOMERS_MIN = 10
CUSTOMERS_MAX = 20
PRODUCTS_MIN = 5
PRODUCTS_MAX = 10

REGIONS = ["Mandera East", "Mandera West", "Mandera Central"]
PAYMENT_STATUSES = ["paid", "failed", "pending"]

PRODUCT_CATEGORIES = {
    "Electronics": [
        "Wireless Earbuds", "Power Bank", "USB-C Hub", "Bluetooth Speaker",
        "Smart Watch", "Laptop Stand", "Webcam", "Portable SSD",
        "Phone Case", "Screen Protector",
    ],
    "Groceries": [
        "Rice 5kg", "Cooking Oil 3L", "Sugar 2kg", "Maize Flour 2kg",
        "Tea Leaves 500g", "Milk Powder 900g", "Salt 1kg",
        "Wheat Flour 2kg", "Pasta 500g", "Tomato Paste",
    ],
    "Clothing": [
        "Cotton T-Shirt", "Denim Jeans", "Polo Shirt", "Hoodie",
        "Khaki Trousers", "Sports Shorts", "Formal Shirt",
        "Beanie Hat", "Canvas Shoes", "Leather Belt",
    ],
    "Home & Kitchen": [
        "Water Bottle", "Thermos Flask", "Frying Pan", "Dinner Set",
        "Storage Container", "Cutting Board", "Blender",
        "Kettle", "Mop Set", "Towel Set",
    ],
}

# ── PostgreSQL / table mappings ─────────────────────────────────────────────────
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
    "database": os.getenv("POSTGRES_DB", "mandera_warehouse"),
    "user": os.getenv("POSTGRES_USER", "pipeline"),
    "password": os.getenv("POSTGRES_PASSWORD", "pipeline_secret"),
}

POSTGRES_URL = (
    f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}"
    f"@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}"
    f"/{POSTGRES_CONFIG['database']}"
)

RAW_TABLES = {
    "customers": "raw.customers_raw",
    "products": "raw.products_raw",
    "orders": "raw.orders_raw",
}

STAGING_TABLES = {
    "customers": "staging.customers_clean",
    "products": "staging.products_clean",
    "orders": "staging.orders_clean",
}


# ── MinIO ──────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "mandera-raw")



def today_partition() -> str:
    """Returns today's date as a MinIO partition path: YYYY/MM/DD/"""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y/%m/%d/")


NUMBER_OF_BATCHES = int(os.getenv("NUMBER_OF_BATCHES", 2))

# Scheduled run hours (UTC) — must match .github/workflows/generate_data.yml cron
BATCH_SCHEDULE_HOURS = [7, 15]

# ── Airflow retry policy ────────────────────────────────────────
AIRFLOW_RETRIES = int(os.getenv("AIRFLOW_RETRIES", 2))
AIRFLOW_RETRY_DELAY = int(os.getenv("AIRFLOW_RETRY_DELAY", 200))  # seconds


def generate_batch_id() -> str:
    """
    Auto-generate batch ID: 2026_03_23_07_batch_1

    Determines the batch number based on which scheduled hour slot
    the current time falls closest to. Falls back to sequential
    numbering if the hour doesn't match a known schedule.
    """
    now = datetime.now(timezone.utc)
    date_part = now.strftime("%Y_%m_%d")
    hour = now.hour

    # Find the closest scheduled hour to determine batch number
    batch_number = 1
    for i, scheduled_hour in enumerate(BATCH_SCHEDULE_HOURS):
        if abs(hour - scheduled_hour) <= 1:
            batch_number = i + 1
            break
    else:
        # Manual run outside scheduled hours — assign based on AM/PM
        batch_number = 1 if hour < 12 else 2

    return f"{date_part}_{hour:02d}_batch_0{batch_number}"