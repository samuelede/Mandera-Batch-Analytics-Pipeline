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
     partitioned by              landing tables
     year/month/day/batch              │
                                       ▼
                              PostgreSQL (staging schema)
                              transformed + validated
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
 
## Project Structure
 
```
mandera_pipeline/
├── generator/
│   ├── data_generator.py       # Main entry point for data generation
│   ├── faker_customers.py      # Customer record generator
│   ├── faker_products.py       # Product record generator
│   └── faker_orders.py         # Order/transaction record generator
├── extraction/
│   ├── extract_mongo_to_minio.py
│   └── extract_mongo_to_postgres.py
├── transformation/
│   ├── transform_customers.py
│   ├── transform_products.py
│   └── transform_orders.py
├── validation/
│   ├── validate_batch_counts.py
│   └── validate_data_quality.py
├── maintenance/
│   └── truncate_raw_tables.py
├── airflow/
│   └── dags/
│       └── mandera_pipeline_dag.py
├── sql/
│   ├── create_raw_tables.sql
│   ├── create_staging_tables.sql
│   └── monitoring_tables.sql
├── docs/
│   ├── data_dictionary.md
│   └── architecture.md
├── .github/
│   └── workflows/
│       └── generate_data.yml
├── .env.example
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
8. Set `MONGO_DB` to the database name the pipeline will use — this does not need to exist in advance. MongoDB Atlas creates it automatically on first write. For this project use:
```
MONGO_DB=mandera_db
```
9. The generator will create three collections automatically on first run: `raw_customers`, `raw_products`, and `raw_orders`.
### 6. Set Up PostgreSQL
 
```bash
psql -U postgres -c "CREATE DATABASE mandera_db;"
psql -U postgres -d mandera_db -f sql/create_raw_tables.sql
psql -U postgres -d mandera_db -f sql/create_staging_tables.sql
psql -U postgres -d mandera_db -f sql/monitoring_tables.sql
```
 
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
 
---
 
## Pipeline Stages
 
| Stage | Script | Description |
|---|---|---|
| Generate | `generator/data_generator.py` | Produce synthetic records with batch ID |
| Ingest | `generator/data_generator.py` | Push records to MongoDB Atlas |
| Extract → Lake | `extraction/extract_mongo_to_minio.py` | Write partitioned Parquet files to MinIO |
| Extract → Warehouse | `extraction/extract_mongo_to_postgres.py` | Load raw tables in PostgreSQL |
| Validate | `validation/validate_batch_counts.py` | Row count and variance monitoring |
| Transform | `transformation/transform_*.py` | Produce staging-ready tables |
| Truncate | `maintenance/truncate_raw_tables.py` | Clear raw tables after staging load |
 
---
 
## Environment Variables
 
See `.env.example` for all required variables.
 
---
 
## GitHub Actions
 
The workflow `.github/workflows/generate_data.yml` runs the data generator on a cron schedule, pushing new batches to MongoDB Atlas automatically.
 
---
 
## License
 
MIT