# config.py — central config for the ETL pipeline

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "sales_db",
    "user":     "postgres",
    "password": "harini123",   # change this
}

RAW_DATA_PATH    = "data/raw_sales.csv"
CLEANED_DATA_DIR = "data/cleaned/"
LOG_FILE         = "etl_pipeline.log"