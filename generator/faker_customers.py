"""
faker_customers.py
------------------
Generates synthetic customer records for the Mandera Analytics pipeline.
Each record is tagged with the current batch_id for traceability.

Bad data injection rates are sourced from config/data_quality.py
so quality thresholds are configured in one place rather than
hardcoded inside the generator.
"""

import sys
import os
import uuid
import random
from faker import Faker

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "config"))
from data_quality import DATA_QUALITY_PROFILE, inject_duplicates

fake = Faker()

CUSTOMER_QUALITY = DATA_QUALITY_PROFILE["customers"]


def introduce_bad_data(customer: dict) -> dict:
    """Introduce realistic data quality issues into customer records.

    Simulates common problems found in real data:
    - Missing emails
    - Invalid emails
    - Missing cities

    Rates are pulled from DATA_QUALITY_PROFILE["customers"] in
    config/data_quality.py rather than hardcoded here.
    """
    if random.random() < CUSTOMER_QUALITY["missing_email_rate"]:
        customer["email"] = None

    if random.random() < CUSTOMER_QUALITY["invalid_email_rate"]:
        customer["email"] = "invalid_email@@"

    if random.random() < CUSTOMER_QUALITY["missing_city_rate"]:
        customer["city"] = ""

    return customer


def generate_customers(batch_id: str, count: int = 100) -> list[dict]:
    """
    Generate a list of synthetic customer records, with a portion of
    records deliberately degraded via introduce_bad_data() to simulate
    real-world data quality issues.

    Args:
        batch_id: Unique identifier for the current pipeline batch.
        count:    Number of customer records to generate.

    Returns:
        List of customer dictionaries ready for MongoDB insertion.
    """
    customers = []

    for _ in range(count):
        customer = {
            "customer_id": str(uuid.uuid4()),
            "batch_id": batch_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "city": fake.city(),
            "country": fake.country(),
            "registration_date": fake.date_between(
                start_date="-3y", end_date="today"
            ).isoformat(),
            "is_active": fake.boolean(chance_of_getting_true=85),
            "segment": fake.random_element(
                elements=["retail", "wholesale", "online"]
            ),
        }

        customer = introduce_bad_data(customer)
        customers.append(customer)

    customers = inject_duplicates(
        customers, id_field="customer_id",
        duplicate_rate=CUSTOMER_QUALITY["duplicate_rate"],
    )

    return customers


# print(generate_customers("2026_03_23_07_batch_1", 15))  # Example usage
