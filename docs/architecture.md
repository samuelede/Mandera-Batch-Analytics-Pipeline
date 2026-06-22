# Architecture — Mandera Analytics Batch Pipeline

## Overview

The Mandera Analytics pipeline is a batch data engineering system
that separates operational data generation from analytical processing
through structured, observable stages. Every table name referenced
below is sourced from `config/settings.py` (`RAW_TABLES`,
`STAGING_TABLES`, `MONGO_COLLECTIONS`) — no script hardcodes a table
or collection name independently.

---

## Pipeline Layers

```
┌─────────────────────────────────────────────────────────────┐
│  LAYER 1 — SOURCE GENERATION                                │
│                                                             │
│  GitHub Actions (Cron, scheduled)                            │
│       │                                                     │
│       ▼                                                     │
│  Python + Faker (generator/)                                 │
│  - generate_batch_id() assigns a human-readable batch_id      │
│    e.g. 2026_03_23_07_batch_01                               │
│  - introduce_bad_data() injects field-level issues per record │
│    (rates defined in config/data_quality.py)                  │
│  - inject_duplicates() injects duplicate records per batch    │
│       │                                                     │
│       ▼                                                     │
│  MongoDB Atlas — settings.MONGO_COLLECTIONS                  │
│  (raw_customers, raw_products, raw_orders)                   │
└─────────────────────────────────────────────────────────────┘
                        │
                        │ Airflow DAG triggered
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 2 — EXTRACTION (parallel)                             │
│                                                             │
│  ┌──────────────────────┐  ┌───────────────────────────┐   │
│  │  MinIO Object Storage│  │  PostgreSQL — raw schema   │   │
│  │                      │  │                           │   │
│  │  mandera-raw/         │  │  raw.customers_raw        │   │
│  │  raw_customers/       │  │  raw.products_raw         │   │
│  │    year=YYYY/         │  │  raw.orders_raw           │   │
│  │    month=MM/          │  │                           │   │
│  │    day=DD/            │  │  (landing zone — exact     │   │
│  │    batch=<id>/        │  │   copy of source, bad      │   │
│  │    raw_customers      │  │   records included)        │   │
│  │      .parquet         │  │                           │   │
│  └──────────────────────┘  └───────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 3 — VALIDATION (batch counts)                          │
│                                                             │
│  validate_batch_counts.py                                   │
│  - Compares MongoDB ↔ raw.*_raw ↔ staging.*_clean row counts │
│  - Flags batches where variance > 5% (VARIANCE_THRESHOLD)    │
│  - Writes to monitoring.batch_row_counts                     │
│  - WARN is expected given deliberate bad-data injection —     │
│    see docs/data_dictionary.md "Data Quality Simulation"      │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 4 — TRANSFORMATION (parallel)                          │
│                                                             │
│  transform_customers.py → staging.customers_clean            │
│  transform_products.py  → staging.products_clean             │
│  transform_orders.py    → staging.orders_clean                │
│                                                             │
│  Each transform:                                              │
│  - Drops records with null primary/foreign keys               │
│  - Removes records that fail entity-specific validity rules   │
│    (negative prices, invalid order_status, etc. — these       │
│    rules exist specifically to catch the injected bad data)   │
│  - Normalises string fields, casts types                      │
│  - products: drops legacy_discount_code (schema-drift field   │
│    not present in the staging schema)                         │
│  - orders: computes net_amount = total_amount − discount      │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 5 — VALIDATION (data quality)                          │
│                                                             │
│  validate_data_quality.py                                    │
│  - Field-level checks on staging.*_clean tables                │
│  - Null checks, duplicate detection, domain value checks       │
│  - Writes to monitoring.data_quality_checks                   │
└─────────────────────────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│  LAYER 6 — MAINTENANCE                                       │
│                                                             │
│  truncate_raw_tables.py                                     │
│  - Confirms staging.*_clean is non-empty for the batch        │
│  - Confirms monitoring.batch_row_counts shows no unresolved   │
│    issues (or proceeds with a logged warning)                 │
│  - Truncates raw.customers_raw, raw.products_raw,              │
│    raw.orders_raw to prepare for the next batch                │
└─────────────────────────────────────────────────────────────┘
```

---

## Airflow DAG Structure

```
start
  └── generate_data
        ├── extract_mongo_to_minio       (parallel)
        └── extract_mongo_to_postgres    (parallel)
              └── validate_batch_counts
                    ├── transform_customers   (parallel)
                    ├── transform_products    (parallel)
                    └── transform_orders      (parallel)
                          └── validate_data_quality
                                └── truncate_raw_tables
                                      └── end
```

Retry policy (`AIRFLOW_RETRIES`, `AIRFLOW_RETRY_DELAY`) is read from
`config/settings.py`, itself sourced from `.env` — not hardcoded in
the DAG file, so retry tuning doesn't require a code change.

---

## MinIO Partition Design

```
mandera-raw/
├── raw_customers/
│   └── year=2026/month=06/day=21/batch=<id>/raw_customers.parquet
├── raw_products/
│   └── year=2026/month=06/day=21/batch=<id>/raw_products.parquet
└── raw_orders/
    └── year=2026/month=06/day=21/batch=<id>/raw_orders.parquet
```

Partition columns: `year`, `month`, `day`, `batch`.

> **Naming note:** MinIO partition folder names follow the MongoDB
> collection names (`raw_customers`, `raw_products`, `raw_orders`),
> not the logical entity names (`customers`, `products`, `orders`)
> used elsewhere in `settings.py`. This is a minor inconsistency
> worth being aware of if extending the lake structure — the
> PostgreSQL `_raw`/`_clean` suffix convention is independent of
> how MinIO folders are named.

This structure enables:
- Time-range pruning for query engines (Trino, Spark)
- Batch-level replay and audit, isolated from any other batch
- Investigation of a specific bad batch without touching others

---

## Naming Conventions Reference

| Concept | Convention | Source of truth |
|---|---|---|
| MongoDB collections | `raw_<entity>` (e.g. `raw_customers`) | `settings.MONGO_COLLECTIONS` |
| PostgreSQL raw tables | `raw.<entity>_raw` (e.g. `raw.customers_raw`) | `settings.RAW_TABLES` |
| PostgreSQL staging tables | `staging.<entity>_clean` (e.g. `staging.customers_clean`) | `settings.STAGING_TABLES` |
| MinIO partition folders | follows MongoDB collection name, not logical entity name | `extract_mongo_to_minio.py` |
| MongoDB database | `mandera_db` (underscore) — **distinct from** the Atlas cluster name `mandera-db` (hyphen) | `settings.MONGO_DB` |

The database-name-vs-cluster-name distinction above caused real
debugging time during initial setup — Atlas Data Explorer's
breadcrumb shows `<cluster> > <database> > <collection>`, and the two
names looking similar (hyphen vs underscore) is coincidental, not
indicative of any actual relationship between them.

---

## Technology Decisions

| Decision | Rationale |
|---|---|
| MongoDB Atlas as source | Simulates operational document store |
| MinIO as object storage | S3-compatible, runs locally without cloud costs |
| PostgreSQL for raw + staging | Familiar SQL warehouse for analytics consumers |
| Pandas for transformation | Lightweight; sufficient for batch sizes in scope |
| Airflow for orchestration | Industry-standard; supports retries, SLA, and monitoring |
| GitHub Actions for generation | Decouples source generation from pipeline |
| Centralised `config/settings.py` | Single source of truth for every table/collection name, preventing the multi-file naming drift encountered during setup |
| `_raw` / `_clean` suffixes | Makes table purpose self-evident from the name alone, independent of schema |
| Deliberate bad-data injection | Gives the validation layer (Layer 3 and Layer 5) something real to detect — without it, quality checks would trivially always pass |

---

## Environment Requirements

| Service | Where it runs |
|---|---|
| MongoDB Atlas | Cloud-hosted (no local install) |
| PostgreSQL 15 | Docker container (`docker-compose.yml`) |
| MinIO | Docker container |
| Redis | Docker container (Celery broker for Airflow) |
| Apache Airflow 2.9+ | Docker containers (webserver, scheduler, worker) |
| pgAdmin | Docker container, optional, pre-configured via `config/pgadmin_servers.json` |

**Host vs. container networking:** scripts run directly from a local
shell (outside Docker) must use `localhost` for `POSTGRES_HOST` and
`MINIO_ENDPOINT` in `.env`. The same scripts, when executed as Airflow
tasks inside the Docker network, use the service names (`postgres`,
`minio`) instead — these only resolve from inside the Docker network,
not from the host machine.