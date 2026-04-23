# main.py — ETL Pipeline Orchestrator

import logging
import sys
import time
from config import DB_CONFIG, RAW_DATA_PATH, LOG_FILE
from etl.extract   import create_spark_session, extract
from etl.transform import transform
from etl.load      import load

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout),
    ]
)
logger = logging.getLogger("main")


def run_pipeline():
    logger.info("=" * 60)
    logger.info("  Sales ETL Pipeline — START")
    logger.info("=" * 60)
    start = time.time()

    spark = None
    try:
        # 1. Extract
        logger.info("--- PHASE 1: EXTRACT ---")
        spark = create_spark_session()
        raw_df = extract(spark, RAW_DATA_PATH)

        # 2. Transform
        logger.info("--- PHASE 2: TRANSFORM ---")
        fact_df, agg_df = transform(raw_df)

        # 3. Load
        logger.info("--- PHASE 3: LOAD ---")
        load(fact_df, agg_df, DB_CONFIG)

        elapsed = round(time.time() - start, 2)
        logger.info("=" * 60)
        logger.info(f"  Pipeline completed successfully in {elapsed}s")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    run_pipeline()