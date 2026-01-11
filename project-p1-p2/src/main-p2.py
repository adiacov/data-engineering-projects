"""Batch Transformation and Data Quality Pipeline"""

import pandas as pd
from sqlalchemy import Engine

from common.db import create_db_engine
from transform.clean import clean
from transform.curated import curate

import logging

logger = logging.getLogger(__name__)


def _clean_dataset(engine: Engine) -> None:
    """ETL clean pipeline.

    Reads dataset from `collisions_raw` table.
    Cleans the dataset.
    Writes cleaned dataset to `collisions_clean` table.
    """

    logger.info("Start ETL pipeline clean phase...")
    try:
        with engine.connect() as conn:
            logger.info("Reading dataset from DB table 'collisions_raw'")
            df = pd.read_sql_table("collisions_raw", conn)

            df = clean(df)

            logger.info("Loading cleaned dataset into 'collisions_clean' DB table")
            df.to_sql(
                "collisions_clean",
                conn,
                if_exists="replace",
                index=False,
            )

    except Exception:
        logger.error("ETL clean pipeline phase failed")
        raise

    logger.info("Successfully executed ETL pipeline clean phase")


def _curate_dataset(engine: Engine) -> None:
    """ETL curated pipeline.

    Reads dataset from `collisions_clean` table.
    Derives business data.
    Writes cleaned dataset to `collisions_curated` table.
    """

    logger.info("Start ETL pipeline curated phase...")

    try:
        with engine.connect() as conn:
            logger.info("Reading dataset from DB table 'collisions_clean'")
            df_clean = pd.read_sql_table(table_name="collisions_clean", con=conn)

            df_curated = curate(df_clean)

            logger.info("Loading cleaned dataset into 'collisions_curated' DB table")
            df_curated.to_sql(
                "collisions_curated",
                conn,
                if_exists="replace",
                index=False,
            )

    except Exception:
        logger.error("ETL curated pipeline phase failed")
        raise

    logger.info("Successfully executed ETL pipeline curated phase")


def main_p2():
    """Pipeline orchestration script that:

    - Reads dataset from collisions_raw table
    - Applies `clean` transformation
    - Writes dataset to collisions_clean table
    - Reads dataset from collisions_clean table
    - Applies `curated` transformation
    - Writes dataset to collisions_curated table
    """

    engine = create_db_engine(echo=False)  # True = display sqlalchemy logs
    _clean_dataset(engine)
    _curate_dataset(engine)


if __name__ == "__main__":
    main_p2()
