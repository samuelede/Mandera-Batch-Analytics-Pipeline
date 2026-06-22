"""
mandera_pipeline_dag.py
-----------------------
Apache Airflow DAG for the Mandera Analytics batch pipeline.

DAG execution order:
    start
      └── generate_data
            ├── extract_mongo_to_minio            (parallel)
            └── extract_mongo_to_postgres         (parallel)
                  ├── validate_batch_counts       (parallel)
                  └── record_batch_log            (parallel)
                        └── transform_customers   (parallel)
                        └── transform_products    (parallel)
                        └── transform_orders      (parallel)
                              └── validate_data_quality
                                    └── truncate_raw_tables
                                          └── end

Schedule: @daily (override via Airflow UI or environment)

Configuration:
    - retries / retry_delay : AIRFLOW_RETRIES / AIRFLOW_RETRY_DELAY,
      read from config/settings.py
    - failure alerts        : on_failure_callback writes a FAILED row
      to monitoring.pipeline_runs (no SMTP/Slack configured — this is
      the alerting mechanism)
    - SLA expectations       : sla= set per task; sla_miss_callback
      writes an SLA_MISS row to monitoring.pipeline_runs

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
from airflow.models.baseoperator import cross_downstream

# ---------------------------------------------------------------------------
# Path setup — MUST run before importing from settings, since settings.py
# is not installed as a package; it's imported via sys.path, same pattern
# used by every other script in this project (generator/, extraction/, etc.)
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.environ.get("MANDERA_PROJECT_ROOT", "/opt/airflow")
for subdir in ["config", "generator", "extraction", "transformation", "validation", "maintenance"]:
    path = os.path.join(PROJECT_ROOT, subdir)
    if path not in sys.path:
        sys.path.insert(0, path)

from settings import AIRFLOW_RETRIES, AIRFLOW_RETRY_DELAY, POSTGRES_URL


# ---------------------------------------------------------------------------
# Failure / SLA callbacks — write to monitoring.pipeline_runs since no
# SMTP/Slack is configured in this project.
# ---------------------------------------------------------------------------
def record_pipeline_failure(context) -> None:
    from sqlalchemy import create_engine, text

    ti = context["task_instance"]
    dag_run = context["dag_run"]
    batch_id = ti.xcom_pull(task_ids="generate_data", key="batch_id") or "unknown"

    engine = create_engine(POSTGRES_URL, echo=False)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO monitoring.pipeline_runs
                    (batch_id, dag_run_id, started_at, completed_at, status, notes)
                VALUES
                    (:batch_id, :dag_run_id, :started_at, :completed_at, 'FAILED', :notes)
            """),
            {
                "batch_id": batch_id,
                "dag_run_id": dag_run.run_id,
                "started_at": dag_run.start_date,
                "completed_at": datetime.utcnow(),
                "notes": f"Task '{ti.task_id}' failed: {context.get('exception')}",
            },
        )
    engine.dispose()


def record_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    from sqlalchemy import create_engine, text

    engine = create_engine(POSTGRES_URL, echo=False)
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO monitoring.pipeline_runs
                    (batch_id, dag_run_id, started_at, completed_at, status, notes)
                VALUES
                    ('unknown', :dag_run_id, :started_at, :completed_at, 'SLA_MISS', :notes)
            """),
            {
                "dag_run_id": dag.dag_id,
                "started_at": datetime.utcnow(),
                "completed_at": datetime.utcnow(),
                "notes": f"SLA missed for task(s): {[t.task_id for t in task_list]}",
            },
        )
    engine.dispose()


# ---------------------------------------------------------------------------
# Default arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "mandera-analytics",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          AIRFLOW_RETRIES,
    "retry_delay":      timedelta(seconds=AIRFLOW_RETRY_DELAY),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": record_pipeline_failure,
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


def task_record_batch_log(ti, **kwargs):
    from record_batch_log import record_batch_log
    batch_id = _resolve_batch_id(ti)
    return record_batch_log(run_id=batch_id)


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
    sla_miss_callback=record_sla_miss,
    tags=["mandera", "batch", "analytics"],
) as dag:

    start = EmptyOperator(task_id="start")

    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=task_generate_data,
        sla=timedelta(minutes=5),
        doc_md="Generate synthetic customer, product, and order records "
               "using Faker and push to MongoDB Atlas.",
    )

    extract_to_minio = PythonOperator(
        task_id="extract_mongo_to_minio",
        python_callable=task_extract_to_minio,
        sla=timedelta(minutes=10),
        doc_md="Extract MongoDB batch records into MinIO object storage "
               "as partitioned Parquet files.",
    )

    extract_to_postgres = PythonOperator(
        task_id="extract_mongo_to_postgres",
        python_callable=task_extract_to_postgres,
        sla=timedelta(minutes=10),
        doc_md="Extract MongoDB batch records into PostgreSQL raw.*_raw "
               "landing tables. Idempotent — clears any existing rows for "
               "the same batch_id before inserting.",
    )

    validate_counts = PythonOperator(
        task_id="validate_batch_counts",
        python_callable=task_validate_counts,
        sla=timedelta(minutes=5),
        doc_md="Compare row counts across MongoDB, raw, and staging layers "
               "(percentage variance, three-stage). Write results to "
               "monitoring.batch_row_counts.",
    )

    record_batch_log = PythonOperator(
        task_id="record_batch_log",
        python_callable=task_record_batch_log,
        sla=timedelta(minutes=5),
        doc_md="Log per-table source (MongoDB) vs loaded (raw.*_raw) row "
               "counts (raw difference, two-stage). Write one row per "
               "entity to monitoring.batch_log.",
    )

    transform_customers = PythonOperator(
        task_id="transform_customers",
        python_callable=task_transform_customers,
        sla=timedelta(minutes=10),
        doc_md="Clean and transform raw.customers_raw → staging.customers_clean.",
    )

    transform_products = PythonOperator(
        task_id="transform_products",
        python_callable=task_transform_products,
        sla=timedelta(minutes=10),
        doc_md="Clean and transform raw.products_raw → staging.products_clean. "
               "Drops legacy_discount_code (schema-drift field) before staging.",
    )

    transform_orders = PythonOperator(
        task_id="transform_orders",
        python_callable=task_transform_orders,
        sla=timedelta(minutes=10),
        doc_md="Clean and transform raw.orders_raw → staging.orders_clean. "
               "Filters invalid order_status values and computes net_amount.",
    )

    validate_quality = PythonOperator(
        task_id="validate_data_quality",
        python_callable=task_validate_quality,
        sla=timedelta(minutes=5),
        doc_md="Run field-level quality checks against staging.*_clean tables.",
    )

    truncate_raw = PythonOperator(
        task_id="truncate_raw_tables",
        python_callable=task_truncate_raw,
        sla=timedelta(minutes=5),
        doc_md="Truncate raw.*_raw tables, but only after verifying every "
               "entity's transformation actually produced staging output.",
    )

    end = EmptyOperator(task_id="end")

    # ---------------------------------------------------------------------------
    # Task dependencies
    # ---------------------------------------------------------------------------
    start >> generate_data

    generate_data >> [extract_to_minio, extract_to_postgres]

    extract_to_postgres >> [validate_counts, record_batch_log]

    cross_downstream([validate_counts, record_batch_log], [transform_customers, transform_products, transform_orders])

    [transform_customers, transform_products, transform_orders] >> validate_quality

    validate_quality >> truncate_raw >> end