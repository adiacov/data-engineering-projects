"""ETL load"""

import pandas as pd
from sqlalchemy import Engine

from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def load_csv_file(file_path: Path, df: pd.DataFrame) -> None:
    """Loads dataset into a file"""
    logger.info(f"Loading dataset into CSV file")
    try:
        df.to_csv(path_or_buf=file_path, index=False)
        logger.info(f"Successfully loaded dataset to {file_path}")
    except Exception as exc:
        logger.error(f"Failed to load dataset to CSV file, because of: {exc}")
        raise


def load_data(df: pd.DataFrame, engine: Engine) -> None:
    """Loads the dataset to database"""
    logger.info("Loading dataset to database...")

    with engine.connect() as con:
        try:
            df.to_sql(
                name="data_projects",
                con=con,
                if_exists="replace",
                index=False,
            )
            logger.info(f"Successfully loaded data into DB")
            con.commit()
        except Exception as exc:
            logger.error("Failed to load data into DB")
            raise
