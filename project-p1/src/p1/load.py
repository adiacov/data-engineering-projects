"""ETL load"""

import pandas as pd
from sqlalchemy import create_engine

from pathlib import Path
import logging

logger = logging.getLogger(__name__)

CLEAN_DATA_FILE = "dft-road-casualty-statistics-collision-2023-clean.csv"
BASE_PATH = Path(__file__).resolve().parents[2]
CLEAN_DATA_FILE_PATH = BASE_PATH / "data" / "processed" / CLEAN_DATA_FILE


def load_csv_file(df: pd.DataFrame) -> None:
    """Loads dataset into a file"""
    logger.info(f"Loading dataset into CSV file")
    try:
        df.to_csv(path_or_buf=CLEAN_DATA_FILE_PATH, index=False)
        logger.info(f"Successfully loaded dataset to {CLEAN_DATA_FILE_PATH}")
    except Exception as exc:
        logger.error(f"Failed to load dataset to CSV file, because of: {exc}")
        raise


def load_data(df: pd.DataFrame) -> None:
    """Loads the dataset to database"""
    logger.info("Loading dataset to database...")

    # creates an in-memory sqlite database
    db_name = "data_engineering.db"
    table_name = "data_projects_p1"
    db_path = f"{Path.home()}/.cache/sqlite/{db_name}"
    db_url = f"sqlite+pysqlite:///{db_path}"

    with create_engine(db_url, echo=False).connect() as con:
        try:
            df.to_sql(
                name="data_projects",
                con=con,
                if_exists="replace",
                index=False,
            )
            logger.info(f"Data loaded into DB at {db_path}, table {table_name}")
        except Exception as exc:
            logger.error("Failed to load data into DB")
            raise
