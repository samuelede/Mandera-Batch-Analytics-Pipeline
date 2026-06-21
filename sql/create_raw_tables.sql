-- =============================================================
-- create_raw_tables.sql
-- Mandera Analytics — PostgreSQL Raw Schema
-- Table names here MUST match config/settings.py RAW_TABLES.
-- If you rename a table here, update RAW_TABLES to match.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- -------------------------------------------------------------
-- raw.customers_raw   (settings.RAW_TABLES["customers"])
-- -------------------------------------------------------------
DROP TABLE IF EXISTS raw.customers_raw;

CREATE TABLE raw.customers_raw (
    customer_id         TEXT            NOT NULL,
    batch_id            TEXT            NOT NULL,
    first_name          TEXT,
    last_name           TEXT,
    email               TEXT,
    phone                TEXT,
    city                TEXT,
    country              TEXT,
    registration_date   TEXT,
    is_active           BOOLEAN,
    segment             TEXT,
    loaded_at           TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (customer_id, batch_id)
);

COMMENT ON TABLE raw.customers_raw IS
    'Raw customer records extracted from MongoDB Atlas. '
    'Preserved before staging transformations. Includes intentional '
    'bad records and duplicates injected by the generator.';

-- -------------------------------------------------------------
-- raw.products_raw   (settings.RAW_TABLES["products"])
-- -------------------------------------------------------------
DROP TABLE IF EXISTS raw.products_raw;

CREATE TABLE raw.products_raw (
    product_id          TEXT            NOT NULL,
    batch_id            TEXT            NOT NULL,
    product_name        TEXT,
    sku                 TEXT,
    category             TEXT,
    unit_price           NUMERIC(12, 2),
    currency             TEXT,
    stock_quantity       INTEGER,
    supplier             TEXT,
    cost_price           NUMERIC(12, 2),
    is_active            BOOLEAN,
    legacy_discount_code TEXT,
    loaded_at            TIMESTAMPTZ    DEFAULT NOW(),
    PRIMARY KEY (product_id, batch_id)
);

COMMENT ON TABLE raw.products_raw IS
    'Raw product catalogue records extracted from MongoDB Atlas. '
    'legacy_discount_code is a schema-drift field injected by the '
    'generator to simulate upstream source changes.';

-- -------------------------------------------------------------
-- raw.orders_raw   (settings.RAW_TABLES["orders"])
-- -------------------------------------------------------------
DROP TABLE IF EXISTS raw.orders_raw;

CREATE TABLE raw.orders_raw (
    order_id            TEXT            NOT NULL,
    batch_id            TEXT            NOT NULL,
    customer_id         TEXT,
    product_id          TEXT,
    quantity             INTEGER,
    unit_price            NUMERIC(12, 2),
    total_amount          NUMERIC(12, 2),
    discount_amount       NUMERIC(12, 2),
    payment_method        TEXT,
    order_status          TEXT,
    region                TEXT,
    order_date            TEXT,
    created_at            TEXT,
    loaded_at             TIMESTAMPTZ    DEFAULT NOW(),
    PRIMARY KEY (order_id, batch_id)
);

COMMENT ON TABLE raw.orders_raw IS
    'Raw order / transaction records extracted from MongoDB Atlas. '
    'Includes intentional bad records (missing product_id, invalid '
    'amounts, invalid status) and duplicates for quality validation.';

-- Indexes for batch-level queries
CREATE INDEX idx_raw_customers_raw_batch  ON raw.customers_raw (batch_id);
CREATE INDEX idx_raw_products_raw_batch   ON raw.products_raw  (batch_id);
CREATE INDEX idx_raw_orders_raw_batch     ON raw.orders_raw    (batch_id);