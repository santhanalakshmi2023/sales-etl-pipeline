# etl/extract.py

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType
)
import logging

logger = logging.getLogger(__name__)

SCHEMA = StructType([
    StructField("sale_id",        StringType(),  True),
    StructField("sale_date",      StringType(),  True),
    StructField("product_id",     StringType(),  True),
    StructField("product_name",   StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("region",         StringType(),  True),
    StructField("sales_rep",      StringType(),  True),
    StructField("quantity",       IntegerType(), True),
    StructField("unit_price",     DoubleType(),  True),
    StructField("unit_cost",      DoubleType(),  True),
    StructField("discount",       DoubleType(),  True),
    StructField("revenue",        DoubleType(),  True),
    StructField("profit",         DoubleType(),  True),
    StructField("payment_method", StringType(),  True),
])

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("SalesETLPipeline")
        .config("spark.sql.shuffle.partitions", "4")   # keep it light for local
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def extract(spark, file_path: str):
    logger.info(f"Extracting data from: {file_path}")
    df = spark.read.csv(
        file_path,
        schema=SCHEMA,
        header=True,
        dateFormat="yyyy-MM-dd"
    )
    count = df.count()
    logger.info(f"Extracted {count} rows from {file_path}")
    df.printSchema()
    return df