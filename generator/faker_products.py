"""
faker_products.py
-----------------
Generates synthetic product catalogue records for the Mandera Analytics
pipeline. Each record is tagged with the current batch_id for traceability.

Imports DATA_QUALITY_PROFILE from data_quality.py and PRODUCT_CATEGORIES
from settings.py. Safe to run standalone — settings.py no longer raises
on import; MONGO_URI is checked only via validate_mongo_config().
"""
 
import sys
import os
import uuid
import random
from faker import Faker
 
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "config"))
from data_quality import DATA_QUALITY_PROFILE, inject_duplicates
from settings import PRODUCT_CATEGORIES
 
fake = Faker()
 
PRODUCT_QUALITY = DATA_QUALITY_PROFILE["products"]
 
 
def introduce_bad_data(product: dict) -> dict:
    """Introduce realistic data quality issues into product records.
 
    Simulates common problems found in real data:
    - Missing product names
    - Invalid prices (negative or non-numeric placeholder)
    - Zero prices
    - Category mismatch (category doesn't match product_name)
    - Schema drift (unexpected extra field)
 
    Rates are pulled from DATA_QUALITY_PROFILE["products"] in
    config/data_quality.py rather than hardcoded here.
    """
    if random.random() < PRODUCT_QUALITY["missing_name_rate"]:
        product["product_name"] = None
 
    if random.random() < PRODUCT_QUALITY["invalid_price_rate"]:
        product["unit_price"] = -1.0
 
    if random.random() < PRODUCT_QUALITY["zero_price_rate"]:
        product["unit_price"] = 0.0
 
    if random.random() < PRODUCT_QUALITY["category_mismatch_rate"]:
        # Assign a category unrelated to the product_name on purpose
        product["category"] = fake.random_element(
            elements=list(PRODUCT_CATEGORIES.keys())
        )
 
    if random.random() < PRODUCT_QUALITY["schema_drift_rate"]:
        # Simulate an upstream schema change introducing an unexpected field
        product["legacy_discount_code"] = fake.bothify(text="OLD-####")
 
    return product
 
 
def generate_products(batch_id: str, count: int = 50) -> list[dict]:
    """
    Generate a list of synthetic product records, with a portion of
    records deliberately degraded via introduce_bad_data() to simulate
    real-world data quality issues.
 
    Args:
        batch_id: Unique identifier for the current pipeline batch.
        count:    Number of product records to generate.
 
    Returns:
        List of product dictionaries ready for MongoDB insertion.
    """
    products = []
    categories = list(PRODUCT_CATEGORIES.keys())
 
    for _ in range(count):
        category = fake.random_element(elements=categories)
        product_name = fake.random_element(elements=PRODUCT_CATEGORIES[category])
        unit_price = round(fake.pyfloat(min_value=1.0, max_value=500.0, right_digits=2), 2)
 
        product = {
            "product_id": str(uuid.uuid4()),
            "batch_id": batch_id,
            "product_name": product_name,
            "sku": fake.bothify(text="SKU-????-####").upper(),
            "category": category,
            "unit_price": unit_price,
            "currency": "USD",
            "stock_quantity": fake.random_int(min=0, max=1000),
            "supplier": fake.company(),
            "cost_price": round(unit_price * 0.6, 2),
            "is_active": fake.boolean(chance_of_getting_true=90),
        }
 
        product = introduce_bad_data(product)
        products.append(product)
 
    products = inject_duplicates(
        products, id_field="product_id",
        duplicate_rate=PRODUCT_QUALITY["duplicate_rate"],
    )
 
    return products

print(generate_products("2026_03_23_07_batch_1", 10))  # Example usage