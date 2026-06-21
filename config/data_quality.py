# config/data_quality.py
#
# Bad-data injection rate profiles used by the faker_*.py generators.

import random
import copy

DATA_QUALITY_PROFILE = {
    "customers": {
        "duplicate_rate": 0.4,
        "missing_email_rate": 0.4,
        "invalid_email_rate": 0.4,
        "missing_city_rate": 0.4
    },
    "orders": {
        "duplicate_rate": 0.4,
        "missing_product_rate": 0.4,
        "invalid_amount_rate": 0.4,
        "negative_amount_rate": 0.4,
        "invalid_status_rate": 0.4
    },
    "products": {
        "duplicate_rate": 0.3,
        "missing_name_rate": 0.42,
        "invalid_price_rate": 0.4,
        "zero_price_rate": 0.4,
        "category_mismatch_rate": 0.4,
        "schema_drift_rate": 0.4
        }
}


def inject_duplicates(records: list[dict], id_field: str, duplicate_rate: float) -> list[dict]:
    """
    Walk through a batch of records and probabilistically replace some
    with a duplicate of an earlier record in the same batch — simulating
    duplicate ingestion from source systems.

    Batch size stays the same: a "duplicated" record keeps its own
    id_field value (so it isn't a true primary-key collision) but
    copies every other field from an earlier record. This mirrors the
    common real-world case of the same customer/order/product being
    re-submitted with a new identifier.

    Args:
        records:        The generated batch, in order.
        id_field:        Name of the primary key field (e.g. "customer_id").
        duplicate_rate:  Probability any given record (after the first)
                         becomes a duplicate of an earlier one in the batch.

    Returns:
        The same list, mutated in place, with some records duplicated.
    """
    for i in range(1, len(records)):
        if random.random() < duplicate_rate:
            source = random.choice(records[:i])
            duplicated = copy.deepcopy(source)
            duplicated[id_field] = records[i][id_field]  # keep original PK
            records[i] = duplicated

    return records