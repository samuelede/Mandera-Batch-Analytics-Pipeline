"""
faker_orders.py
---------------
Generates synthetic order / transaction records for Mandera Analytics.
Orders reference customer_ids and product_ids from the same batch.
Each record is tagged with the current batch_id for traceability.
"""

import uuid
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

PAYMENT_METHODS = ["credit_card", "debit_card", "bank_transfer", "cash", "mobile_pay"]
ORDER_STATUSES = ["completed", "pending", "cancelled", "refunded"]
REGIONS = ["North", "South", "East", "West", "Central"]


def generate_orders(
    batch_id: str,
    customer_ids: list[str],
    product_ids: list[str],
    count: int = 200,
) -> list[dict]:
    """
    Generate a list of synthetic order / transaction records.

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

        orders.append(
            {
                "order_id": str(uuid.uuid4()),
                "batch_id": batch_id,
                "customer_id": fake.random_element(elements=customer_ids),
                "product_id": fake.random_element(elements=product_ids),
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount,
                # Intentional bad record injection (~3% negative amounts)
                "discount_amount": fake.random_element(
                    elements=[
                        round(total_amount * 0.05, 2),
                        0.0,
                        round(total_amount * 0.10, 2),
                        -1.0,  # bad record
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
        )

    return orders
