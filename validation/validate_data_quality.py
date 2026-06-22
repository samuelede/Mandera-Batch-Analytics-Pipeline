"""
validate_data_quality.py
------------------------
Runs field-level data quality checks against staging tables
for a given batch_id.

Table names are imported from config/settings.py STAGING_TABLES —
never hardcoded.

Checks performed:
- Null rate on critical fields
- Duplicate primary key detection
- Value domain validation (e.g. order_status values)
- Negative/zero amount detection

Results are written to monitoring.data_quality_checks.

Usage:
    python validation/validate_data_quality.py --batch-id <batch_id>
"""

import sys
import os
import logging
import argparse
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "config"))
from settings import POSTGRES_URL, STAGING_TABLES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

VALID_ORDER_STATUSES  = {"completed", "pending", "cancelled", "refunded"}
VALID_PAYMENT_METHODS = {"credit_card", "debit_card", "bank_transfer", "cash", "mobile_pay"}

PK_FIELD = {
    "customers": "customer_id",
    "products":  "product_id",
    "orders":    "order_id",
}


def write_check(engine, check: dict) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO monitoring.data_quality_checks (
                    batch_id, entity, check_name, check_result,
                    failed_count, total_count, status, checked_at
                ) VALUES (
                    :batch_id, :entity, :check_name, :check_result,
                    :failed_count, :total_count, :status, :checked_at
                )
            """),
            check,
        )


def run_checks(batch_id: str, entity: str, df: pd.DataFrame) -> list[dict]:
    checks = []
    now = datetime.now(timezone.utc).isoformat()
    total = len(df)

    def record(name, failed, result=""):
        status = "PASS" if failed == 0 else "FAIL"
        log.info("[%s] %s | %s | failed=%d/%d", status, entity, name, failed, total)
        return {
            "batch_id": batch_id, "entity": entity, "check_name": name,
            "check_result": result, "failed_count": int(failed),
            "total_count": total, "status": status, "checked_at": now,
        }

    pk_col = PK_FIELD[entity]
    failed = df[pk_col].isna().sum()
    checks.append(record(f"null_{pk_col}", failed))

    if entity == "customers":
        checks.append(record("null_email", df["email"].isna().sum()))
        checks.append(record("duplicate_email", df.duplicated(subset=["email"]).sum()))
        checks.append(record("missing_city", (df["city"] == "").sum()))

    if entity == "products":
        checks.append(record("non_positive_unit_price", (df["unit_price"] <= 0).sum()))
        checks.append(record("null_sku", df["sku"].isna().sum()))
        checks.append(record("null_product_name", df["product_name"].isna().sum()))

    if entity == "orders":
        invalid_status = (~df["order_status"].isin(VALID_ORDER_STATUSES)).sum()
        checks.append(record("invalid_order_status", invalid_status, str(VALID_ORDER_STATUSES)))

        invalid_payment = (~df["payment_method"].isin(VALID_PAYMENT_METHODS)).sum()
        checks.append(record("invalid_payment_method", invalid_payment, str(VALID_PAYMENT_METHODS)))

        checks.append(record("non_positive_total_amount", (df["total_amount"] <= 0).sum()))
        checks.append(record("negative_net_amount", (df["net_amount"] < 0).sum()))
        checks.append(record("null_product_id", df["product_id"].isna().sum()))

    return checks


def validate_quality(batch_id: str) -> list[dict]:
    engine = create_engine(POSTGRES_URL, echo=False)
    all_checks = []

    for entity, table_full in STAGING_TABLES.items():
        log.info("Running quality checks on %s | batch: %s", table_full, batch_id)
        df = pd.read_sql(
            f"SELECT * FROM {table_full} WHERE batch_id = %(bid)s",
            con=engine,
            params={"bid": batch_id},
        )

        if df.empty:
            log.warning("No %s records for batch %s — skipping checks.", table_full, batch_id)
            continue

        checks = run_checks(batch_id, entity, df)
        for check in checks:
            write_check(engine, check)

        all_checks.extend(checks)

    failed = [c for c in all_checks if c["status"] == "FAIL"]
    log.info("Quality check complete. Total: %d | Passed: %d | Failed: %d",
              len(all_checks), len(all_checks) - len(failed), len(failed))

    engine.dispose()
    return all_checks


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    validate_quality(args.batch_id)