"""Batch Transformation and Data Quality Pipeline"""

import pandas as pd

from common.db import create_db_engine
from transform.clean import clean

import logging

logger = logging.getLogger(__name__)


def main_p2():
    """Pipeline orchestration script that:

    - Reads dataset from collisions_raw table
    - Applies `clean` transformation
    - Writes dataset to collisions_clean table
    - Reads dataset from collisions_clean table
    - Applies `curated` transformation
    - Writes dataset to collisions_curated table
    """

    try:
        engine = create_db_engine()

        with engine.connect() as conn:
            logger.info("Creating dataset from DB table 'collisions_raw'")
            df = pd.read_sql_table("collisions_raw", conn)

            df = clean(df)

            logger.info("Loading cleaned dataset into 'collisions_clean' DB table")
            df.to_sql(
                "collisions_clean",
                conn,
                if_exists="replace",
                index=False,
            )

        logger.info("Successfully executed ETL pipeline")

    except Exception:
        logger.error("ETL pipeline failed")
        raise


if __name__ == "__main__":
    main_p2()
