-- gold/ddl.sql
-- Gold layer star schema DDL for Fabric Lakehouse SQL endpoint
-- Execute via Fabric SQL query editor or scripts/validate_lakehouse.py

-- ── Dimension: Products ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold_dim_product (
    product_id              VARCHAR(64)         NOT NULL,
    product_name            VARCHAR(256),
    category                VARCHAR(64),
    sub_category            VARCHAR(64),
    brand                   VARCHAR(128),
    unit_cost               DECIMAL(10, 2),
    list_price              DECIMAL(10, 2),
    gross_margin            DECIMAL(10, 2),
    margin_pct              DECIMAL(5, 2),
    is_active               BIT,
    _gold_processed_at      DATETIME2
)
WITH (CLUSTERED COLUMNSTORE INDEX);

-- ── Dimension: Stores ──────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold_dim_store (
    store_id                VARCHAR(64)         NOT NULL,
    store_name              VARCHAR(256),
    region                  VARCHAR(64),
    state                   VARCHAR(64),
    city                    VARCHAR(128),
    store_type              VARCHAR(32),
    open_date               DATE,
    is_active               BIT,
    _gold_processed_at      DATETIME2
)
WITH (CLUSTERED COLUMNSTORE INDEX);

-- ── Dimension: Date ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold_dim_date (
    date_key                INT                 NOT NULL,   -- YYYYMMDD
    full_date               DATE                NOT NULL,
    day_of_week             TINYINT,
    day_name                VARCHAR(10),
    week_of_year            TINYINT,
    month_num               TINYINT,
    month_name              VARCHAR(10),
    quarter                 TINYINT,
    year                    SMALLINT,
    is_weekend              BIT,
    is_holiday              BIT,
    fiscal_period           VARCHAR(10),        -- e.g. FY2024-Q1
    PRIMARY KEY NONCLUSTERED (date_key) NOT ENFORCED
)
WITH (CLUSTERED COLUMNSTORE INDEX);

-- ── Fact: Orders ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold_fct_orders (
    order_id                VARCHAR(64)         NOT NULL,
    customer_id_hash        VARCHAR(64)         NOT NULL,
    store_id                VARCHAR(64),
    product_id              VARCHAR(64),
    region                  VARCHAR(64),
    state                   VARCHAR(64),
    category                VARCHAR(64),
    quantity                INT,
    unit_price              DECIMAL(10, 2),
    order_amount            DECIMAL(12, 2),
    estimated_margin        DECIMAL(12, 2),
    order_date              DATE,
    order_year              INT,
    order_month             INT,
    status                  VARCHAR(32),
    _gold_processed_at      DATETIME2
)
WITH (CLUSTERED COLUMNSTORE INDEX);

-- ── Fact: Revenue (aggregated) ─────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold_fct_revenue (
    order_date              DATE                NOT NULL,
    region                  VARCHAR(64),
    state                   VARCHAR(64),
    store_id                VARCHAR(64),
    category                VARCHAR(64),
    order_year              INT,
    order_month             INT,
    total_revenue           DECIMAL(14, 2),
    total_margin            DECIMAL(14, 2),
    total_orders            INT,
    unique_customers        INT,
    avg_order_value         DECIMAL(10, 2),
    units_sold              INT,
    margin_pct              DECIMAL(5, 2),
    _gold_processed_at      DATETIME2
)
WITH (CLUSTERED COLUMNSTORE INDEX);

-- ── Fact: Customer LTV ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS gold_fct_customer_ltv (
    customer_id_hash        VARCHAR(64)         NOT NULL,
    lifetime_value          DECIMAL(14, 2),
    total_orders            INT,
    avg_order_value         DECIMAL(10, 2),
    active_days             INT,
    first_order_date        DATE,
    last_order_date         DATE,
    customer_tenure_days    INT,
    ltv_tier                VARCHAR(16),        -- Bronze / Silver / Gold / Platinum
    _gold_processed_at      DATETIME2
)
WITH (CLUSTERED COLUMNSTORE INDEX);

-- ── Views for Semantic Model ───────────────────────────────────────────────────

CREATE OR ALTER VIEW vw_revenue_by_region AS
SELECT
    r.order_date,
    r.region,
    r.state,
    s.store_name,
    r.category,
    r.total_revenue,
    r.total_margin,
    r.margin_pct,
    r.total_orders,
    r.unique_customers,
    r.avg_order_value,
    r.units_sold
FROM gold_fct_revenue r
LEFT JOIN gold_dim_store s ON r.store_id = s.store_id;

CREATE OR ALTER VIEW vw_product_performance AS
SELECT
    o.order_date,
    p.product_name,
    p.category,
    p.brand,
    p.margin_pct AS product_margin_pct,
    SUM(o.order_amount)     AS total_revenue,
    SUM(o.estimated_margin) AS total_margin,
    SUM(o.quantity)         AS units_sold,
    COUNT(o.order_id)       AS order_count
FROM gold_fct_orders o
LEFT JOIN gold_dim_product p ON o.product_id = p.product_id
WHERE o.status != 'CANCELLED'
GROUP BY o.order_date, p.product_name, p.category, p.brand, p.margin_pct;
