-- =============================================================
-- create_staging_tables.sql
-- Mandera Analytics — PostgreSQL Staging Schema
-- Table names here MUST match config/settings.py STAGING_TABLES.
-- If you rename a table here, update STAGING_TABLES to match.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- -------------------------------------------------------------
-- staging.customers_clean   (settings.STAGING_TABLES["customers"])
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging.customers_clean;

CREATE TABLE staging.customers_clean (
    customer_id         TEXT            NOT NULL,
    batch_id            TEXT            NOT NULL,
    first_name          TEXT,
    last_name           TEXT,
    email               TEXT            NOT NULL,
    phone               TEXT,
    city                TEXT,
    country             TEXT,
    registration_date   DATE,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    segment             TEXT            NOT NULL DEFAULT 'unknown',
    transformed_at      TIMESTAMPTZ,
    loaded_at           TIMESTAMPTZ,
    PRIMARY KEY (customer_id, batch_id)
);

COMMENT ON TABLE staging.customers_clean IS
    'Cleaned and validated customer records. Null/invalid emails '
    'removed, missing cities flagged, duplicates resolved. '
    'Registration date cast to DATE.';

-- -------------------------------------------------------------
-- staging.products_clean   (settings.STAGING_TABLES["products"])
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging.products_clean;

CREATE TABLE staging.products_clean (
    product_id          TEXT            NOT NULL,
    batch_id            TEXT            NOT NULL,
    product_name        TEXT            NOT NULL,
    sku                 TEXT            NOT NULL,
    category            TEXT,
    unit_price          NUMERIC(12, 2)  NOT NULL,
    currency            TEXT            DEFAULT 'USD',
    stock_quantity      INTEGER,
    supplier            TEXT,
    cost_price          NUMERIC(12, 2),
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    transformed_at      TIMESTAMPTZ,
    loaded_at           TIMESTAMPTZ,
    PRIMARY KEY (product_id, batch_id)
);

COMMENT ON TABLE staging.products_clean IS
    'Cleaned product records. Null product_name and non-positive '
    'prices removed. legacy_discount_code (schema drift field) is '
    'dropped during transformation — not carried into staging.';

-- -------------------------------------------------------------
-- staging.orders_clean   (settings.STAGING_TABLES["orders"])
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging.orders_clean;

CREATE TABLE staging.orders_clean (
    order_id            TEXT            NOT NULL,
    batch_id            TEXT            NOT NULL,
    customer_id         TEXT            NOT NULL,
    product_id          TEXT            NOT NULL,
    quantity            INTEGER,
    unit_price          NUMERIC(12, 2),
    total_amount        NUMERIC(12, 2)  NOT NULL,
    discount_amount     NUMERIC(12, 2)  DEFAULT 0.00,
    net_amount          NUMERIC(12, 2),
    payment_method      TEXT,
    order_status        TEXT,
    region              TEXT,
    order_date          TIMESTAMPTZ,
    created_at          TEXT,
    transformed_at      TIMESTAMPTZ,
    loaded_at           TIMESTAMPTZ,
    PRIMARY KEY (order_id, batch_id)
);

COMMENT ON TABLE staging.orders_clean IS
    'Cleaned order / transaction records. Missing product_id, '
    'invalid/negative amounts, and invalid statuses removed. '
    'net_amount computed after discount.';

-- Indexes for reporting queries
CREATE INDEX idx_stg_customers_clean_batch  ON staging.customers_clean (batch_id);
CREATE INDEX idx_stg_products_clean_batch   ON staging.products_clean  (batch_id);
CREATE INDEX idx_stg_orders_clean_batch     ON staging.orders_clean    (batch_id);
CREATE INDEX idx_stg_orders_clean_customer  ON staging.orders_clean    (customer_id);
CREATE INDEX idx_stg_orders_clean_product   ON staging.orders_clean    (product_id);
CREATE INDEX idx_stg_orders_clean_date      ON staging.orders_clean    (order_date);
CREATE INDEX idx_stg_orders_clean_region    ON staging.orders_clean    (region);
CREATE INDEX idx_stg_orders_clean_status    ON staging.orders_clean    (order_status);