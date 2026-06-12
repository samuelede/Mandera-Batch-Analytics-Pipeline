"""
mandera_pipeline_dag.py
-----------------------
Apache Airflow DAG for the Mandera Analytics batch pipeline.

DAG execution order:
    start
      └── extract_mongo_to_minio
      └── extract_mongo_to_postgres
            └── validate_batch_counts
                  └── transform_customers
                  └── transform_products
                  └── transform_orders
                        └── validate_data_quality
                              └── truncate_raw_tables
                                    └── end

Schedule: @daily (override via Airflow UI or environment)

Configuration:
    Ensure the following Airflow Variables are set:
      - BATCH_ID (optional — defaults to latest MongoDB batch)

    Ensure the following Airflow Connections are configured:
      - mongo_atlas    (MongoDB URI)
      - postgres_main  (PostgreSQL connection)
      - minio_conn     (MinIO / S3 connection)
"""

import sys
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

# ---------------------------------------------------------------------------
# Path setup — allows importing project modules from Airflow tasks
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.environ.get("MANDERA_PROJECT_ROOT", "/opt/airflow/mandera_pipeline")
for subdir in ["generator", "extraction", "transformation", "validation", "maintenance"]:
    path = os.path.join(PROJECT_ROOT, subdir)
    if path not in sys.path:
        sys.path.insert(0, path)

# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "mandera-analytics",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# Helper — resolve batch_id from XCom or Airflow Variable
# ---------------------------------------------------------------------------
def _resolve_batch_id(ti) -> str:
    """Pull batch_id pushed by the generate task, or fall back to Variable."""
    batch_id = ti.xcom_pull(task_ids="generate_data", key="batch_id")
    if not batch_id:
        batch_id = Variable.get("BATCH_ID", default_var=None)
    if not batch_id:
        raise ValueError(
            "batch_id not found in XCom or Airflow Variables. "
            "Ensure the generate_data task ran successfully."
        )
    return batch_id


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
def task_generate_data(ti, **kwargs):
    from data_generator import run_generator
    summary = run_generator()
    ti.xcom_push(key="batch_id", value=summary["batch_id"])
    ti.xcom_push(key="generation_summary", value=summary)
    return summary


def task_extract_to_minio(ti, **kwargs):
    from extract_mongo_to_minio import run_extraction
    batch_id = _resolve_batch_id(ti)
    return run_extraction(batch_id=batch_id)


def task_extract_to_postgres(ti, **kwargs):
    from extract_mongo_to_postgres import run_extraction
    batch_id = _resolve_batch_id(ti)
    return run_extraction(batch_id=batch_id)


def task_validate_counts(ti, **kwargs):
    from validate_batch_counts import validate_batch
    batch_id = _resolve_batch_id(ti)
    return validate_batch(batch_id=batch_id)


def task_transform_customers(ti, **kwargs):
    from transform_customers import transform_customers
    batch_id = _resolve_batch_id(ti)
    return transform_customers(batch_id=batch_id)


def task_transform_products(ti, **kwargs):
    from transform_products import transform_products
    batch_id = _resolve_batch_id(ti)
    return transform_products(batch_id=batch_id)


def task_transform_orders(ti, **kwargs):
    from transform_orders import transform_orders
    batch_id = _resolve_batch_id(ti)
    return transform_orders(batch_id=batch_id)


def task_validate_quality(ti, **kwargs):
    from validate_data_quality import validate_quality
    batch_id = _resolve_batch_id(ti)
    return validate_quality(batch_id=batch_id)


def task_truncate_raw(ti, **kwargs):
    from truncate_raw_tables import truncate_raw_tables
    batch_id = _resolve_batch_id(ti)
    return truncate_raw_tables(batch_id=batch_id)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="mandera_batch_pipeline",
    default_args=default_args,
    description="Mandera Analytics end-to-end batch data pipeline",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["mandera", "batch", "analytics"],
) as dag:

    start = EmptyOperator(task_id="start")

    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=task_generate_data,
        doc_md="Generate synthetic customer, product, and order records "
               "using Faker and push to MongoDB Atlas.",
    )

    extract_to_minio = PythonOperator(
        task_id="extract_mongo_to_minio",
        python_callable=task_extract_to_minio,
        doc_md="Extract MongoDB batch records into MinIO object storage "
               "as partitioned Parquet files.",
    )

    extract_to_postgres = PythonOperator(
        task_id="extract_mongo_to_postgres",
        python_callable=task_extract_to_postgres,
        doc_md="Extract MongoDB batch records into PostgreSQL raw schema landing tables.",
    )

    validate_counts = PythonOperator(
        task_id="validate_batch_counts",
        python_callable=task_validate_counts,
        doc_md="Compare row counts across MongoDB, raw, and staging layers. "
               "Write results to monitoring.batch_row_counts.",
    )

    transform_customers = PythonOperator(
        task_id="transform_customers",
        python_callable=task_transform_customers,
        doc_md="Clean and transform raw.customers → staging.customers.",
    )

    transform_products = PythonOperator(
        task_id="transform_products",
        python_callable=task_transform_products,
        doc_md="Clean and transform raw.products → staging.products.",
    )

    transform_orders = PythonOperator(
        task_id="transform_orders",
        python_callable=task_transform_orders,
        doc_md="Clean and transform raw.orders → staging.orders.",
    )

    validate_quality = PythonOperator(
        task_id="validate_data_quality",
        python_callable=task_validate_quality,
        doc_md="Run field-level quality checks against staging tables.",
    )

    truncate_raw = PythonOperator(
        task_id="truncate_raw_tables",
        python_callable=task_truncate_raw,
        doc_md="Truncate raw schema tables after successful staging load.",
    )

    end = EmptyOperator(task_id="end")

    # ---------------------------------------------------------------------------
    # Task dependencies
    # ---------------------------------------------------------------------------
    start >> generate_data

    generate_data >> [extract_to_minio, extract_to_postgres]

    extract_to_postgres >> validate_counts

    validate_counts >> [transform_customers, transform_products, transform_orders]

    [transform_customers, transform_products, transform_orders] >> validate_quality

    validate_quality >> truncate_raw >> end
