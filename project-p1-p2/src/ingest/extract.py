"""ETL extract"""

import pandas as pd

from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def read_csv_file(file_path: Path) -> pd.DataFrame:
    """Returns a dataset from a CSV file"""
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Reading raw dataset CSV file: {file_path}")
        return df
    except FileNotFoundError as exc:
        logger.error(f"File not found {file_path}")
        raise
    except Exception as exc:
        logger.error(f"Could not create DataFrame, because of {exc}")
        raise
