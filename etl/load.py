# etl/load.py

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)


def get_connection(db_config: dict):
    conn = psycopg2.connect(**db_config)
    conn.autocommit = False
    return conn


def upsert_fact_sales(df: DataFrame, db_config: dict):
    """Load cleaned fact_sales rows — upsert on sale_id."""
    rows = df.collect()
    logger.info(f"Loading {len(rows)} rows into fact_sales...")

    sql = """
        INSERT INTO fact_sales (
            sale_id, sale_date, product_id, product_name, category,
            region, sales_rep, quantity, unit_price, unit_cost,
            discount, revenue, profit, profit_margin, payment_method
        ) VALUES %s
        ON CONFLICT (sale_id) DO UPDATE SET
            revenue       = EXCLUDED.revenue,
            profit        = EXCLUDED.profit,
            profit_margin = EXCLUDED.profit_margin,
            loaded_at     = NOW();
    """
    data = [
        (
            r.sale_id, r.sale_date, r.product_id, r.product_name, r.category,
            r.region, r.sales_rep, r.quantity, r.unit_price, r.unit_cost,
            r.discount, r.revenue, r.profit, r.profit_margin, r.payment_method
        )
        for r in rows
    ]

    conn = get_connection(db_config)
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, data, page_size=200)
        conn.commit()
        logger.info(f"fact_sales: {len(data)} rows upserted successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"fact_sales load failed: {e}")
        raise
    finally:
        conn.close()


def upsert_agg_daily(df: DataFrame, db_config: dict):
    """Load daily aggregated summary — upsert on (sale_date, region, category)."""
    rows = df.collect()
    logger.info(f"Loading {len(rows)} rows into agg_daily_sales...")

    sql = """
        INSERT INTO agg_daily_sales (
            sale_date, region, category,
            total_orders, total_quantity, total_revenue, total_profit, avg_discount
        ) VALUES %s
        ON CONFLICT (sale_date, region, category) DO UPDATE SET
            total_orders   = EXCLUDED.total_orders,
            total_quantity = EXCLUDED.total_quantity,
            total_revenue  = EXCLUDED.total_revenue,
            total_profit   = EXCLUDED.total_profit,
            avg_discount   = EXCLUDED.avg_discount,
            loaded_at      = NOW();
    """
    data = [
        (
            r.sale_date, r.region, r.category,
            r.total_orders, r.total_quantity,
            r.total_revenue, r.total_profit, r.avg_discount
        )
        for r in rows
    ]

    conn = get_connection(db_config)
    try:
        with conn.cursor() as cur:
            execute_values(cur, sql, data, page_size=200)
        conn.commit()
        logger.info(f"agg_daily_sales: {len(data)} rows upserted successfully.")
    except Exception as e:
        conn.rollback()
        logger.error(f"agg_daily_sales load failed: {e}")
        raise
    finally:
        conn.close()


def load(fact_df: DataFrame, agg_df: DataFrame, db_config: dict):
    upsert_fact_sales(fact_df, db_config)
    upsert_agg_daily(agg_df, db_config)
    logger.info("All data loaded into PostgreSQL successfully.")