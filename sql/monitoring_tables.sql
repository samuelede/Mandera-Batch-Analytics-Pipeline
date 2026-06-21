-- =============================================================
-- monitoring_tables.sql
-- Mandera Analytics — Pipeline Monitoring Schema
-- NOTE: entity values stored here ("customers", "products", "orders")
-- are logical names, NOT physical table names. They are unaffected
-- by the raw/staging _raw / _clean suffix convention.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS monitoring;

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
    'entity is a logical name resolved to physical tables via '
    'config/settings.py RAW_TABLES and STAGING_TABLES.';

CREATE INDEX idx_mon_counts_batch   ON monitoring.batch_row_counts (batch_id);
CREATE INDEX idx_mon_counts_entity  ON monitoring.batch_row_counts (entity);
CREATE INDEX idx_mon_counts_status  ON monitoring.batch_row_counts (status);

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
    'Individual data quality check results run against staging tables.';

CREATE INDEX idx_mon_dq_batch   ON monitoring.data_quality_checks (batch_id);
CREATE INDEX idx_mon_dq_entity  ON monitoring.data_quality_checks (entity);
CREATE INDEX idx_mon_dq_status  ON monitoring.data_quality_checks (status);

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
    'High-level audit log for each pipeline execution.';

CREATE INDEX idx_mon_runs_batch   ON monitoring.pipeline_runs (batch_id);
CREATE INDEX idx_mon_runs_status  ON monitoring.pipeline_runs (status);