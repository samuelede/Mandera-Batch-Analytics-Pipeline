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

## Important: Where Each Service Runs

**MongoDB Atlas is cloud-hosted** — reached over the internet via `MONGO_URI`, no local setup needed beyond an Atlas account.

**PostgreSQL, MinIO, Redis, pgAdmin, and Airflow all run as Docker containers**, defined in `docker-compose.yml`. They are not installed natively on your host machine.

This matters because of **hostname resolution**:
- Inside the Docker network, services reach each other by container name: `postgres`, `minio`, `redis`.
- From your host shell (e.g. running `python extraction/extract_mongo_to_postgres.py` directly in Git Bash), those names don't resolve — Windows has no machine called `postgres` or `minio`. You must use `localhost` instead when testing scripts standalone, then switch back to the service names when those scripts run as Airflow tasks inside the Docker network.

See Section 8 below for the exact `.env` values to use in each case.

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

Duplicates are injected at the batch level (after generation), since duplication is inherently a cross-record concern — `inject_duplicates()` walks the generated batch and probabilistically overwrites later records with a copy of an earlier one's fields, keeping the original primary key.

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
├── docker-compose.yml           # PostgreSQL, MinIO, Redis, pgAdmin, Airflow containers
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
6. Once the cluster is provisioned, click **Connect** → **Drivers**, select **Python**, and copy the connection string.
7. Paste it into your `.env` as `MONGO_URI`.
8. Set `MONGO_DB` to the actual database name visible in Atlas Data Explorer — **note: this is the database name, not the cluster name.** Cluster names and database names are independent; Atlas lets a cluster called `mandera-db` contain a database called `mandera_db`, for example. Always confirm in Atlas Data Explorer rather than assuming they match.
9. Set `MONGO_COLLECTIONS` (in `config/settings.py`) to match the actual collection names `data_generator.py` writes to — verify these exist in Atlas Data Explorer before running extraction.

### 6. Start Docker Services (PostgreSQL, MinIO, Redis, Airflow)

#### 6.1 Generate a Fernet key for Airflow

`.env.example` ships with a placeholder — Airflow needs a real encryption key or it will fail to start:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Paste the output into `.env`:
```
AIRFLOW__CORE__FERNET_KEY=<paste_generated_key_here>
```

#### 6.2 Start PostgreSQL, MinIO, and Redis first

```bash
docker compose up -d postgres minio redis
docker compose ps   # wait until all three report "healthy"
```

#### 6.3 Create the warehouse schema (run once)

`psql` runs **inside** the `postgres` container — there is no `psql` installed on your host. The `sql/` folder is mounted read-only into the container at `/sql`, so run it via `docker exec`:

```bash
# If mandera_warehouse doesn't exist yet:
docker exec -it mandera-postgres psql -U pipeline -c "CREATE DATABASE mandera_warehouse;"

docker exec -it mandera-postgres psql -U pipeline -d mandera_warehouse -f /sql/create_raw_tables.sql
docker exec -it mandera-postgres psql -U pipeline -d mandera_warehouse -f /sql/create_staging_tables.sql
docker exec -it mandera-postgres psql -U pipeline -d mandera_warehouse -f /sql/monitoring_tables.sql
```

#### 6.4 Initialize Airflow's metadata database (run once)

```bash
docker compose run --rm airflow-webserver airflow db init

docker compose run --rm airflow-webserver airflow users create \
  --username admin --password admin \
  --firstname Mandera --lastname Admin \
  --role Admin --email admin@mandera.com
```

#### 6.5 Start Airflow

```bash
docker compose up -d airflow-webserver airflow-scheduler airflow-worker
```

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 |
| MinIO Console | http://localhost:9001 |
| pgAdmin | http://localhost:5050 |

### 7. Run Data Generator Manually

```bash
python generator/data_generator.py
```

You can also test each Faker generator standalone with zero `.env` setup, since they only depend on `config/data_quality.py`:

```bash
python generator/faker_customers.py
python generator/faker_products.py
python generator/faker_orders.py
```

### 8. Running Pipeline Scripts From Your Host Shell (Standalone Testing)

If you run `extraction/`, `transformation/`, `validation/`, or `maintenance/` scripts **directly from your shell** rather than as Airflow tasks, your `.env` must point to `localhost`, not Docker service names — `postgres`, `minio`, and `redis` only resolve from **inside** the Docker network.

For standalone host-side testing, temporarily set in `.env`:
```
POSTGRES_HOST=localhost
MINIO_ENDPOINT=http://localhost:9000
```

Switch them back to `postgres` / `minio` once these scripts run as Airflow tasks inside the Docker network — the DAG containers resolve those names correctly because they're on the same Docker network.

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