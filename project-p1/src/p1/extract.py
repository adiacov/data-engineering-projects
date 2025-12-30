"""ETL extract"""

import pandas as pd

from pathlib import Path
import logging

logger = logging.getLogger(__name__)

RAW_DATA_FILE = "dft-road-casualty-statistics-collision-2023.csv"
BASE_PATH = Path(__file__).resolve().parents[2]
RAW_DATA_FILE_PATH = BASE_PATH / "data" / "raw" / RAW_DATA_FILE


def read_csv_file() -> pd.DataFrame:
    """Returns a dataset from a CSV file"""
    try:
        df = pd.read_csv(RAW_DATA_FILE_PATH)
        logger.info(f"Reading raw dataset CSV file: {RAW_DATA_FILE_PATH}")
        return df
    except FileNotFoundError as exc:
        logger.error(f"File not found {RAW_DATA_FILE_PATH}")
        raise
    except Exception as exc:
        logger.error(f"Could not create DataFrame, because of {exc}")
        raise
