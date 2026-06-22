-- =============================================================
-- monitoring_tables.sql
-- Mandera Analytics — Pipeline Monitoring Schema
-- Tracks batch row counts, variance, and data quality checks.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS monitoring;

-- -------------------------------------------------------------
-- monitoring.batch_row_counts
-- Per-entity, three-stage (MongoDB / raw / staging) row count
-- comparison, with variance expressed as a PERCENTAGE.
-- -------------------------------------------------------------
DROP TABLE IF EXISTS monitoring.batch_row_counts;

CREATE TABLE monitoring.batch_row_counts (
    id              SERIAL          PRIMARY KEY,
    batch_id        TEXT            NOT NULL,
    entity          TEXT            NOT NULL,       -- customers | products | orders
    mongo_count     INTEGER         NOT NULL DEFAULT 0,
    raw_count       INTEGER         NOT NULL DEFAULT 0,
    staging_count   INTEGER         NOT NULL DEFAULT 0,
    variance_pct    NUMERIC(6, 4)   NOT NULL DEFAULT 0.0000,
    status          TEXT            NOT NULL DEFAULT 'OK',   -- OK | WARN
    recorded_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE monitoring.batch_row_counts IS
    'Per-batch row count audit across MongoDB, raw, and staging layers. '
    'variance_pct = (raw_count - staging_count) / raw_count. '
    'status = WARN if variance_pct > 0.05.';

CREATE INDEX idx_mon_counts_batch   ON monitoring.batch_row_counts (batch_id);
CREATE INDEX idx_mon_counts_entity  ON monitoring.batch_row_counts (entity);
CREATE INDEX idx_mon_counts_status  ON monitoring.batch_row_counts (status);

-- -------------------------------------------------------------
-- monitoring.batch_log
-- Per-entity, two-stage (source vs loaded) row count log, with
-- variance expressed as a RAW ROW COUNT DIFFERENCE. One row per
-- table per run — simpler companion to batch_row_counts above.
-- -------------------------------------------------------------
CREATE TABLE IF NOT EXISTS monitoring.batch_log (
    id            SERIAL PRIMARY KEY,
    run_id        VARCHAR(50)  NOT NULL,
    table_name    VARCHAR(100) NOT NULL,
    batch_date    DATE         NOT NULL DEFAULT CURRENT_DATE,
    source_rows   INTEGER      NOT NULL,
    loaded_rows   INTEGER      NOT NULL,
    variance      INTEGER      NOT NULL,
    load_time     TIMESTAMP    NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE monitoring.batch_log IS
    'Per-run, per-table row count log. One row is written for each '
    'raw.*_raw table on every extraction run. variance = source_rows - loaded_rows. '
    'Populated by maintenance/record_batch_log.py.';

COMMENT ON COLUMN monitoring.batch_log.run_id IS
    'Pipeline batch_id this row belongs to.';

COMMENT ON COLUMN monitoring.batch_log.table_name IS
    'Target raw table this row tracks, e.g. raw.customers_raw.';

COMMENT ON COLUMN monitoring.batch_log.source_rows IS
    'Document count in the corresponding MongoDB collection for this run_id.';

COMMENT ON COLUMN monitoring.batch_log.loaded_rows IS
    'Row count actually loaded into table_name for this run_id.';

COMMENT ON COLUMN monitoring.batch_log.variance IS
    'source_rows - loaded_rows. Zero means every source record was loaded successfully.';

CREATE INDEX IF NOT EXISTS idx_batch_log_run_id     ON monitoring.batch_log (run_id);
CREATE INDEX IF NOT EXISTS idx_batch_log_table_name ON monitoring.batch_log (table_name);
CREATE INDEX IF NOT EXISTS idx_batch_log_batch_date ON monitoring.batch_log (batch_date);

-- -------------------------------------------------------------
-- monitoring.data_quality_checks
-- Individual field-level quality check results per batch.
-- -------------------------------------------------------------
DROP TABLE IF EXISTS monitoring.data_quality_checks;

CREATE TABLE monitoring.data_quality_checks (
    id              SERIAL          PRIMARY KEY,
    batch_id        TEXT            NOT NULL,
    entity          TEXT            NOT NULL,
    check_name      TEXT            NOT NULL,
    check_result    TEXT,
    failed_count    INTEGER         NOT NULL DEFAULT 0,
    total_count     INTEGER         NOT NULL DEFAULT 0,
    status          TEXT            NOT NULL DEFAULT 'PASS',  -- PASS | FAIL
    checked_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE monitoring.data_quality_checks IS
    'Individual data quality check results run against staging tables. '
    'failed_count = number of records that failed the check. '
    'status = FAIL if failed_count > 0.';

CREATE INDEX idx_mon_dq_batch   ON monitoring.data_quality_checks (batch_id);
CREATE INDEX idx_mon_dq_entity  ON monitoring.data_quality_checks (entity);
CREATE INDEX idx_mon_dq_status  ON monitoring.data_quality_checks (status);

-- -------------------------------------------------------------
-- monitoring.pipeline_runs
-- High-level log of each Airflow DAG run.
-- -------------------------------------------------------------
DROP TABLE IF EXISTS monitoring.pipeline_runs;

CREATE TABLE monitoring.pipeline_runs (
    id              SERIAL          PRIMARY KEY,
    batch_id        TEXT            NOT NULL,
    dag_run_id      TEXT,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    status          TEXT            NOT NULL DEFAULT 'RUNNING', -- RUNNING | SUCCESS | FAILED
    customers_raw   INTEGER         DEFAULT 0,
    products_raw    INTEGER         DEFAULT 0,
    orders_raw      INTEGER         DEFAULT 0,
    notes           TEXT,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE monitoring.pipeline_runs IS
    'High-level audit log for each pipeline execution. '
    'Updated by the Airflow DAG at start and completion.';

CREATE INDEX idx_mon_runs_batch   ON monitoring.pipeline_runs (batch_id);
CREATE INDEX idx_mon_runs_status  ON monitoring.pipeline_runs (status);