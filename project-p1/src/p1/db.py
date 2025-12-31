"""SQL engine management"""

from sqlalchemy import create_engine

from pathlib import Path
import logging

logger = logging.getLogger(__file__)


def create_engine():
    # Declaring here for simplicity. Usually these are kept in config files
    db_name = "data_engineering.db"
    db_path = f"{Path.home()}/.cache/sqlite/{db_name}"
    db_url = f"sqlite+pysqlite:///{db_path}"

    try:
        logging.info("Creating SQL engine")
        create_engine(db_url, echo=True)
        logging.info("Successfully created SQL engine")
    except Exception:
        logging.error("Failed to create SQL engine")
        raise
