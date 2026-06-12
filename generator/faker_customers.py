"""
faker_customers.py
------------------
Generates synthetic customer records for Mandera Analytics.
Each record is tagged with the current batch_id for traceability.
"""

import uuid
from faker import Faker

fake = Faker()


def generate_customers(batch_id: str, count: int = 100) -> list[dict]:
    """
    Generate a list of synthetic customer records.

    Args:
        batch_id: Unique identifier for the current pipeline batch.
        count:    Number of customer records to generate.

    Returns:
        List of customer dictionaries ready for MongoDB insertion.
    """
    customers = []

    for _ in range(count):
        customers.append(
            {
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
                # Intentional bad record injection (~2% of records)
                "segment": fake.random_element(
                    elements=["retail", "wholesale", "online", None, ""]
                ),
            }
        )

    return customers
