# etl/transform.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
import logging

logger = logging.getLogger(__name__)

def clean(df: DataFrame) -> DataFrame:
    """Drop nulls, cast types, fill defaults."""
    logger.info("Starting cleaning step...")
    initial = df.count()

    df = (
        df
        # Cast sale_date properly
        .withColumn("sale_date", F.to_date("sale_date", "yyyy-MM-dd"))
        # Fill missing region/sales_rep with 'Unknown'
        .withColumn("region",    F.coalesce(F.col("region"),    F.lit("Unknown")))
        .withColumn("sales_rep", F.coalesce(F.col("sales_rep"), F.lit("Unknown")))
        # Drop rows with no sale_id or no sale_date
        .dropna(subset=["sale_id", "sale_date"])
        # Remove duplicates
        .dropDuplicates(["sale_id"])
        # Ensure numeric sanity: quantity & prices must be positive
        .filter(F.col("quantity")   > 0)
        .filter(F.col("unit_price") > 0)
        .filter(F.col("unit_cost")  > 0)
        .filter((F.col("discount") >= 0) & (F.col("discount") <= 1))
    )

    cleaned = df.count()
    logger.info(f"Cleaning done: {initial} -> {cleaned} rows (dropped {initial - cleaned})")
    return df


def add_derived_columns(df: DataFrame) -> DataFrame:
    """Add profit_margin, year, month, quarter."""
    logger.info("Adding derived columns...")
    df = (
        df
        .withColumn(
            "profit_margin",
            F.round((F.col("profit") / F.col("revenue")) * 100, 2)
        )
        .withColumn("year",    F.year("sale_date"))
        .withColumn("month",   F.month("sale_date"))
        .withColumn("quarter", F.quarter("sale_date"))
    )
    return df


def aggregate_daily(df: DataFrame) -> DataFrame:
    """Aggregate to daily + region + category level."""
    logger.info("Aggregating to daily summary...")
    agg = (
        df.groupBy("sale_date", "region", "category")
        .agg(
            F.count("sale_id")         .alias("total_orders"),
            F.sum("quantity")          .alias("total_quantity"),
            F.round(F.sum("revenue"),2).alias("total_revenue"),
            F.round(F.sum("profit"), 2).alias("total_profit"),
            F.round(F.avg("discount"),2).alias("avg_discount"),
        )
        .orderBy("sale_date", "region", "category")
    )
    logger.info(f"Aggregation produced {agg.count()} summary rows")
    return agg


def transform(df: DataFrame):
    """Run full transform — returns (fact_df, agg_df)."""
    df = clean(df)
    df = add_derived_columns(df)
    agg_df = aggregate_daily(df)

    # Select only the columns we need for fact_sales
    fact_df = df.select(
        "sale_id", "sale_date", "product_id", "product_name",
        "category", "region", "sales_rep", "quantity",
        "unit_price", "unit_cost", "discount",
        "revenue", "profit", "profit_margin", "payment_method"
    )
    return fact_df, agg_df