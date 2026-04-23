-- sql/create_tables.sql
-- Run this once before the pipeline

CREATE DATABASE sales_db;

\c sales_db;

-- Raw / cleaned fact table
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id        VARCHAR(10) PRIMARY KEY,
    sale_date      DATE        NOT NULL,
    product_id     VARCHAR(10) NOT NULL,
    product_name   VARCHAR(100),
    category       VARCHAR(50),
    region         VARCHAR(50),
    sales_rep      VARCHAR(100),
    quantity       INTEGER,
    unit_price     NUMERIC(10,2),
    unit_cost      NUMERIC(10,2),
    discount       NUMERIC(5,2),
    revenue        NUMERIC(12,2),
    profit         NUMERIC(12,2),
    profit_margin  NUMERIC(5,2),
    payment_method VARCHAR(50),
    loaded_at      TIMESTAMP DEFAULT NOW()
);

-- Daily aggregated summary
CREATE TABLE IF NOT EXISTS agg_daily_sales (
    sale_date      DATE        NOT NULL,
    region         VARCHAR(50) NOT NULL,
    category       VARCHAR(50) NOT NULL,
    total_orders   INTEGER,
    total_quantity INTEGER,
    total_revenue  NUMERIC(14,2),
    total_profit   NUMERIC(14,2),
    avg_discount   NUMERIC(5,2),
    loaded_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (sale_date, region, category)
);