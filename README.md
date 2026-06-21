# Mandera-Batch-Analytics-Pipeline
End-to-end batch analytics pipeline using MongoDB Atlas, MinIO, PostgreSQL, Apache Airflow, and GitHub Actions. Covers synthetic data generation, object storage partitioning, raw-to-staging warehouse layering, transformation, quality validation, and batch monitoring.

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9+-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB%20Atlas-47A248?style=flat-square&logo=mongodb&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-4169E1?style=flat-square&logo=postgresql&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-Object%20Storage-C72E49?style=flat-square&logo=minio&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub%20Actions-Scheduled-2088FF?style=flat-square&logo=githubactions&logoColor=white)

A production-style batch analytics pipeline simulating how operational transaction data moves through structured engineering layers — from synthetic data generation to analytics-ready staging tables.

---

## Architecture Overview

```
GitHub Actions (Scheduled)
        │
        ▼
Python + Faker ──► MongoDB Atlas (Operational Source)
                          │
              ┌───────────┴───────────┐
              ▼                       ▼
     MinIO Object Storage      PostgreSQL (raw schema)
     partitioned by              raw.*_raw landing tables
     year/month/day/batch              │
                                       ▼
                              PostgreSQL (staging schema)
                              staging.*_clean — transformed + validated
```

---

## Technology Stack

| Layer | Tool |
|---|---|
| Data Generation | Python, Faker |
| Scheduling | GitHub Actions |
| Operational Source | MongoDB Atlas |
| Object Storage | MinIO |
| Warehouse | PostgreSQL |
| Transformation | Pandas |
| Orchestration | Apache Airflow |

---

## Configuration

All connection strings, table name mappings, and pipeline constants live in `config/settings.py` — no module hardcodes a DSN or table name directly. Bad-data injection rates live separately in `config/data_quality.py`, which has no database awareness at all, so the `faker_*.py` generators can run standalone with zero `.env` setup.

| File | Purpose |
|---|---|
| `config/settings.py` | MongoDB/PostgreSQL/MinIO connection config, `RAW_TABLES`/`STAGING_TABLES` name mappings, batch ID generation |
| `config/data_quality.py` | Per-entity bad-data injection rates, shared `inject_duplicates()` helper |
| `config/pgadmin_servers.json` | Pre-configured pgAdmin connection to the PostgreSQL Docker container |

`settings.py` does **not** raise on import if `MONGO_URI` is missing — that check is deferred to `validate_mongo_config()`, called explicitly only by scripts that open a live MongoDB connection (the generator entry point, extraction scripts).

---

## Data Quality Simulation

The generator deliberately injects realistic data problems into every batch, controlled centrally via `DATA_QUALITY_PROFILE` in `config/data_quality.py`:

| Entity | Issues injected |
|---|---|
| Customers | Missing emails, invalid emails, missing cities, duplicate records |
| Products | Missing names, invalid/zero prices, category mismatches, schema drift (`legacy_discount_code`), duplicate records |
| Orders | Missing `product_id`, invalid/negative `total_amount`, invalid `order_status`, duplicate records |

Duplicates are injected at the batch level (after generation) rather than per-record, since duplication is inherently a cross-record concern — `inject_duplicates()` walks the generated batch and probabilistically overwrites later records with a copy of an earlier one's fields, keeping the original primary key.

This is what makes the validation layer meaningful — `validate_data_quality.py` and `validate_batch_counts.py` exist specifically to catch these issues downstream.

> **Note:** with duplicate injection plus multiple bad-record categories compounding, raw → staging variance will often exceed the default 5% `VARIANCE_THRESHOLD` in `validate_batch_counts.py`. This is expected given the deliberately noisy generator — `WARN` status here is informational, not necessarily a pipeline failure.

---

## Project Structure

```
mandera_pipeline/
├── config/
│   ├── settings.py             # Central config: connections, table names, batch ID logic
│   ├── data_quality.py         # Bad-data injection rates + inject_duplicates() helper
│   └── pgadmin_servers.json    # pgAdmin auto-connection config for Docker PostgreSQL
├── generator/
│   ├── data_generator.py       # Main entry point — generates records, pushes to MongoDB Atlas
│   ├── faker_customers.py      # Customer record generator
│   ├── faker_products.py       # Product record generator
│   └── faker_orders.py         # Order/transaction record generator
├── extraction/
│   ├── extract_mongo_to_minio.py       # Extracts by batch_id → partitioned Parquet in MinIO
│   └── extract_mongo_to_postgres.py    # Loads raw.*_raw landing tables in PostgreSQL
├── transformation/
│   ├── transform_customers.py  # Cleans raw.customers_raw → staging.customers_clean
│   ├── transform_products.py   # Cleans raw.products_raw → staging.products_clean
│   └── transform_orders.py     # Cleans raw.orders_raw → staging.orders_clean
├── validation/
│   ├── validate_batch_counts.py    # Row count comparison across MongoDB, raw, and staging
│   └── validate_data_quality.py    # Field-level checks: nulls, duplicates, domain values
├── maintenance/
│   └── truncate_raw_tables.py  # Safety-checked truncation of raw.*_raw tables after staging load
├── airflow/
│   └── dags/
│       └── mandera_pipeline_dag.py     # Full Airflow DAG with task dependencies and XCom batch_id
├── sql/
│   ├── create_raw_tables.sql       # raw.customers_raw / products_raw / orders_raw
│   ├── create_staging_tables.sql   # staging.customers_clean / products_clean / orders_clean
│   └── monitoring_tables.sql       # batch_row_counts, data_quality_checks, pipeline_runs
├── docs/
│   ├── data_dictionary.md      # Field definitions, types, and data quality rules
│   └── architecture.md         # Layer diagram, partition design, technology decisions
├── .github/
│   └── workflows/
│       └── generate_data.yml   # Scheduled GitHub Actions cron for daily data generation
├── .env.example                # Environment variable template — safe to commit
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/mandera-batch-analytics-pipeline.git
cd mandera-batch-analytics-pipeline
```

### 2. Create Virtual Environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS / Linux
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

```bash
cp .env.example .env
# Edit .env with your credentials
```

### 5. Set Up MongoDB Atlas

1. Go to [cloud.mongodb.com](https://cloud.mongodb.com) and sign in or create a free account.
2. Create a new project, then click **Build a Database** and select the **M0 Free** tier.
3. Choose a cloud provider and region closest to you, then click **Create**.
4. Under **Security → Database Access**, create a database user with a username and password — save these for your `.env`.
5. Under **Security → Network Access**, click **Add IP Address** → **Allow Access from Anywhere** (`0.0.0.0/0`) for local development.
6. Once the cluster is provisioned, click **Connect** → **Drivers**, select **Python**, and copy the connection string. It will look like:
```
mongodb+srv://<username>:<password>@<cluster>.mongodb.net/?retryWrites=true&w=majority&appName=<AppName>
```
7. Paste it into your `.env` as `MONGO_URI`, replacing `<username>` and `<password>` with your database user credentials.
8. Set `MONGO_DB` to the database name the pipeline will use:
```
MONGO_DB=mandera-db
```
9. The generator will create three collections automatically on first run, as defined in `config/settings.py` → `MONGO_COLLECTIONS`: `customers`, `products`, `orders`.

### 6. Set Up PostgreSQL

```bash
psql -U postgres -c "CREATE DATABASE mandera_warehouse;"
psql -U postgres -d mandera_warehouse -f sql/create_raw_tables.sql
psql -U postgres -d mandera_warehouse -f sql/create_staging_tables.sql
psql -U postgres -d mandera_warehouse -f sql/monitoring_tables.sql
```

This creates `raw.customers_raw`, `raw.products_raw`, `raw.orders_raw` and their `staging.*_clean` counterparts — matching the `RAW_TABLES`/`STAGING_TABLES` mappings in `config/settings.py`. If you ever rename a table in the SQL files, update `settings.py` to match, or every downstream script will break.

### 7. Start MinIO (Docker)

```bash
docker run -d \
  --name mandera-minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin123 \
  -v minio_data:/data \
  quay.io/minio/minio server /data --console-address ":9001"
```

Access MinIO Console: http://localhost:9001

### 8. Set Up Apache Airflow

```bash
export AIRFLOW_HOME=$(pwd)/airflow

airflow db init

airflow users create \
  --username admin \
  --password admin \
  --firstname Mandera \
  --lastname Admin \
  --role Admin \
  --email admin@mandera.com

# Copy DAG
cp airflow/dags/mandera_pipeline_dag.py $AIRFLOW_HOME/dags/

# Start services
airflow webserver --port 8080 &
airflow scheduler &
```

Access Airflow UI: http://localhost:8080

### 9. Run Data Generator Manually

```bash
python generator/data_generator.py
```

You can also test each Faker generator standalone with zero `.env` setup, since they only depend on `config/data_quality.py`:

```bash
python generator/faker_customers.py
python generator/faker_products.py
python generator/faker_orders.py
```

---

## Pipeline Stages

| Stage | Script | Description |
|---|---|---|
| Generate | `generator/data_generator.py` | Produce synthetic records with batch ID and injected data quality issues |
| Ingest | `generator/data_generator.py` | Push records to MongoDB Atlas |
| Extract → Lake | `extraction/extract_mongo_to_minio.py` | Write partitioned Parquet files to MinIO |
| Extract → Warehouse | `extraction/extract_mongo_to_postgres.py` | Load `raw.*_raw` tables in PostgreSQL |
| Validate Counts | `validation/validate_batch_counts.py` | Row count and variance monitoring across MongoDB, raw, and staging |
| Transform | `transformation/transform_*.py` | Clean raw records into `staging.*_clean` tables |
| Validate Quality | `validation/validate_data_quality.py` | Field-level checks: nulls, duplicates, domain values |
| Truncate | `maintenance/truncate_raw_tables.py` | Clear `raw.*_raw` tables after staging load |

---

## Environment Variables

See `.env.example` for all required variables.

---

## GitHub Actions

The workflow `.github/workflows/generate_data.yml` runs the data generator on a cron schedule, pushing new batches to MongoDB Atlas automatically.

---

## License

MIT