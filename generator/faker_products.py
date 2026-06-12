"""
faker_products.py
-----------------
Generates synthetic product catalogue records for Mandera Analytics.
Each record is tagged with the current batch_id for traceability.
"""

import uuid
from faker import Faker

fake = Faker()

CATEGORIES = [
    "Electronics",
    "Apparel",
    "Groceries",
    "Home & Garden",
    "Sports",
    "Automotive",
    "Health",
    "Toys",
]


def generate_products(batch_id: str, count: int = 50) -> list[dict]:
    """
    Generate a list of synthetic product records.

    Args:
        batch_id: Unique identifier for the current pipeline batch.
        count:    Number of product records to generate.

    Returns:
        List of product dictionaries ready for MongoDB insertion.
    """
    products = []

    for _ in range(count):
        unit_price = round(fake.pyfloat(min_value=1.0, max_value=500.0, right_digits=2), 2)

        products.append(
            {
                "product_id": str(uuid.uuid4()),
                "batch_id": batch_id,
                "product_name": fake.catch_phrase(),
                "sku": fake.bothify(text="SKU-????-####").upper(),
                "category": fake.random_element(elements=CATEGORIES),
                "unit_price": unit_price,
                "currency": "USD",
                "stock_quantity": fake.random_int(min=0, max=1000),
                "supplier": fake.company(),
                # Intentional bad record injection (~3% null prices)
                "cost_price": fake.random_element(
                    elements=[
                        round(unit_price * 0.6, 2),
                        round(unit_price * 0.65, 2),
                        None,
                    ]
                ),
                "is_active": fake.boolean(chance_of_getting_true=90),
            }
        )

    return products
