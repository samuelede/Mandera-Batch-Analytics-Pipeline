"""
faker_orders.py
---------------
Generates synthetic order / transaction records for the Mandera Analytics
pipeline. Orders reference customer_ids and product_ids from the same batch.
Each record is tagged with the current batch_id for traceability.

Bad data injection rates are sourced from config/data_quality.py
so quality thresholds are configured in one place rather than
hardcoded inside the generator.
"""

import sys
import os
import uuid
import random
from datetime import datetime, timezone
from faker import Faker

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "config"))
from data_quality import DATA_QUALITY_PROFILE, inject_duplicates

fake = Faker()

ORDER_QUALITY = DATA_QUALITY_PROFILE["orders"]

PAYMENT_METHODS = ["credit_card", "debit_card", "bank_transfer", "cash", "mobile_pay"]
ORDER_STATUSES = ["completed", "pending", "cancelled", "refunded"]
REGIONS = ["North", "South", "East", "West", "Central"]


def introduce_bad_data(order: dict) -> dict:
    """Introduce realistic data quality issues into order records.

    Simulates common problems found in real data:
    - Missing product_id (order references a product that wasn't captured)
    - Invalid total_amount (non-sensical value)
    - Negative total_amount
    - Invalid order_status (value outside the accepted domain)

    Rates are pulled from DATA_QUALITY_PROFILE["orders"] in
    config/data_quality.py rather than hardcoded here.
    """
    if random.random() < ORDER_QUALITY["missing_product_rate"]:
        order["product_id"] = None

    if random.random() < ORDER_QUALITY["invalid_amount_rate"]:
        order["total_amount"] = None

    if random.random() < ORDER_QUALITY["negative_amount_rate"]:
        order["total_amount"] = -abs(order.get("total_amount") or 1.0)

    if random.random() < ORDER_QUALITY["invalid_status_rate"]:
        order["order_status"] = "unknown_status"

    return order


def generate_orders(
    batch_id: str,
    customer_ids: list[str],
    product_ids: list[str],
    count: int = 200,
) -> list[dict]:
    """
    Generate a list of synthetic order / transaction records, with a portion
    of records deliberately degraded via introduce_bad_data() to simulate
    real-world data quality issues.

    Args:
        batch_id:     Unique identifier for the current pipeline batch.
        customer_ids: List of customer_ids generated in the same batch.
        product_ids:  List of product_ids generated in the same batch.
        count:        Number of order records to generate.

    Returns:
        List of order dictionaries ready for MongoDB insertion.
    """
    orders = []

    for _ in range(count):
        quantity = fake.random_int(min=1, max=10)
        unit_price = round(fake.pyfloat(min_value=5.0, max_value=300.0, right_digits=2), 2)
        total_amount = round(quantity * unit_price, 2)

        order = {
            "order_id": str(uuid.uuid4()),
            "batch_id": batch_id,
            "customer_id": fake.random_element(elements=customer_ids),
            "product_id": fake.random_element(elements=product_ids),
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": total_amount,
            "discount_amount": fake.random_element(
                elements=[
                    round(total_amount * 0.05, 2),
                    0.0,
                    round(total_amount * 0.10, 2),
                ]
            ),
            "payment_method": fake.random_element(elements=PAYMENT_METHODS),
            "order_status": fake.random_element(elements=ORDER_STATUSES),
            "region": fake.random_element(elements=REGIONS),
            "order_date": fake.date_time_between(
                start_date="-30d", end_date="now", tzinfo=timezone.utc
            ).isoformat(),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        order = introduce_bad_data(order)
        orders.append(order)

    orders = inject_duplicates(
        orders, id_field="order_id",
        duplicate_rate=ORDER_QUALITY["duplicate_rate"],
    )

    return orders


if __name__ == "__main__":
    # Standalone test run — works with zero .env setup since this
    # file has no MongoDB/Postgres/MinIO dependency at all.
    # Dummy IDs stand in for customer_ids/product_ids normally
    # supplied by data_generator.py after generating those entities first.
    dummy_customer_ids = [str(uuid.uuid4()) for _ in range(5)]
    dummy_product_ids  = [str(uuid.uuid4()) for _ in range(5)]
    print(generate_orders("2026_03_23_07_batch_1", dummy_customer_ids, dummy_product_ids, 10))

# print(generate_orders("2026_03_23_07_batch_1", ["cust1", "cust2"], ["prod1", "prod2"], 15))  # Example usage