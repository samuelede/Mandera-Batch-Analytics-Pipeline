-- =============================================================
-- create_staging_tables.sql
-- Mandera Analytics — PostgreSQL Staging Schema
-- Staging tables contain cleaned, validated, analytics-ready data.
-- They are the primary source for downstream reporting.
-- =============================================================

CREATE SCHEMA IF NOT EXISTS staging;

-- -------------------------------------------------------------
-- staging.customers_clean
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging.customers_clean;

CREATE TABLE staging.customers_clean (
    customer_id           TEXT            NOT NULL,
    batch_id              TEXT            NOT NULL,
    first_name            TEXT,
    last_name             TEXT,
    email                 TEXT            NOT NULL,
    phone                 TEXT,
    city                  TEXT,
    country               TEXT,
    registration_date     DATE,
    is_active             BOOLEAN         NOT NULL DEFAULT TRUE,
    segment               TEXT            NOT NULL DEFAULT 'unknown',
    full_name             TEXT,
    customer_tenure_days  INTEGER,
    transformed_at        TIMESTAMPTZ,
    loaded_at             TIMESTAMPTZ,
    PRIMARY KEY (customer_id, batch_id)
);

COMMENT ON TABLE staging.customers_clean IS
    'Cleaned and validated customer records. Missing/invalid-format '
    'emails replaced with a placeholder. Segment normalised. '
    'Registration date cast to DATE. full_name and '
    'customer_tenure_days are derived fields computed during '
    'transformation, not sourced from raw.customers_raw directly.';

COMMENT ON COLUMN staging.customers_clean.email IS
    'Lowercased, format-validated. Missing or malformed values '
    '(including the invalid_email_rate injected bad records) are '
    'replaced with unknown@unknown.com rather than dropped.';

COMMENT ON COLUMN staging.customers_clean.segment IS
    'Customer segment: retail | wholesale | online | unknown';

COMMENT ON COLUMN staging.customers_clean.full_name IS
    'Derived: first_name || '' '' || last_name.';

COMMENT ON COLUMN staging.customers_clean.customer_tenure_days IS
    'Derived: days elapsed between registration_date and transform run time.';

-- -------------------------------------------------------------
-- staging.products_clean
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging.products_clean;

CREATE TABLE staging.products_clean (
    product_id          TEXT            NOT NULL,
    batch_id             TEXT            NOT NULL,
    product_name        TEXT,
    sku                  TEXT            NOT NULL,
    category             TEXT,
    unit_price           NUMERIC(12, 2)  NOT NULL,
    currency              TEXT            DEFAULT 'USD',
    stock_quantity         INTEGER,
    supplier               TEXT,
    cost_price             NUMERIC(12, 2),
    is_active               BOOLEAN         NOT NULL DEFAULT TRUE,
    margin_pct              NUMERIC(6, 4),
    price_band               TEXT,
    category_verified        BOOLEAN,
    transformed_at            TIMESTAMPTZ,
    loaded_at                 TIMESTAMPTZ,
    PRIMARY KEY (product_id, batch_id)
);

COMMENT ON TABLE staging.products_clean IS
    'Cleaned product records. Non-positive prices dropped (cannot be '
    'repaired without fabricating a value). Null names/SKU/category '
    'replaced with placeholders. legacy_discount_code (schema-drift '
    'field present in raw.products_raw) is intentionally NOT carried '
    'into this table. margin_pct, price_band, and category_verified '
    'are derived fields computed during transformation.';

COMMENT ON COLUMN staging.products_clean.category_verified IS
    'FALSE if category does not match the expected category for '
    'product_name (per config/settings.py PRODUCT_CATEGORIES). The '
    'category value itself is left as-is — the true category is not '
    'recoverable once mismatched by the generator; this flag tells '
    'downstream consumers the field is unreliable for that row.';

COMMENT ON COLUMN staging.products_clean.margin_pct IS
    'Derived: (unit_price - cost_price) / unit_price.';

COMMENT ON COLUMN staging.products_clean.price_band IS
    'Derived: low (<50) / mid (<200) / high (>=200), based on unit_price.';

-- -------------------------------------------------------------
-- staging.orders_clean
-- -------------------------------------------------------------
DROP TABLE IF EXISTS staging.orders_clean;

CREATE TABLE staging.orders_clean (
    order_id           TEXT            NOT NULL,
    batch_id            TEXT            NOT NULL,
    customer_id          TEXT            NOT NULL,
    product_id           TEXT            NOT NULL,
    quantity              INTEGER,
    unit_price             NUMERIC(12, 2),
    total_amount            NUMERIC(12, 2)  NOT NULL,
    discount_amount          NUMERIC(12, 2)  DEFAULT 0.00,
    net_amount                NUMERIC(12, 2),
    discount_applied           BOOLEAN,
    payment_method               TEXT,
    order_status                  TEXT,
    region                         TEXT,
    order_date                      TIMESTAMPTZ,
    created_at                       TEXT,
    transformed_at                    TIMESTAMPTZ,
    loaded_at                          TIMESTAMPTZ,
    PRIMARY KEY (order_id, batch_id)
);

COMMENT ON TABLE staging.orders_clean IS
    'Cleaned order / transaction records. Rows with missing identity '
    'fields, non-positive total_amount, or an order_status outside '
    'the valid domain are excluded (cannot be repaired without '
    'fabricating a financial figure or an outcome that did not '
    'happen). net_amount and discount_applied are derived fields '
    'computed during transformation.';

COMMENT ON COLUMN staging.orders_clean.net_amount IS
    'Derived: total_amount minus discount_amount.';

COMMENT ON COLUMN staging.orders_clean.discount_applied IS
    'Derived: TRUE if discount_amount > 0.';

-- Indexes for reporting queries
CREATE INDEX idx_stg_customers_clean_batch    ON staging.customers_clean (batch_id);
CREATE INDEX idx_stg_products_clean_batch     ON staging.products_clean  (batch_id);
CREATE INDEX idx_stg_orders_clean_batch       ON staging.orders_clean    (batch_id);
CREATE INDEX idx_stg_orders_clean_customer    ON staging.orders_clean    (customer_id);
CREATE INDEX idx_stg_orders_clean_product     ON staging.orders_clean    (product_id);
CREATE INDEX idx_stg_orders_clean_date        ON staging.orders_clean    (order_date);
CREATE INDEX idx_stg_orders_clean_region      ON staging.orders_clean    (region);
CREATE INDEX idx_stg_orders_clean_status      ON staging.orders_clean    (order_status);
CREATE INDEX idx_stg_products_clean_category_verified ON staging.products_clean (category_verified);