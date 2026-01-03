"""SQL engine management"""

from sqlalchemy import create_engine, Engine

from pathlib import Path
import logging

logger = logging.getLogger(__file__)


def create_db_engine(echo: bool = False) -> Engine:
    """Creates a new database engine instance.

    :param echo=False: if True, the Engine will log all statements
    """
    # Declaring here for simplicity. Usually these are kept in config files
    db_name = "data_engineering.db"
    db_path = f"{Path.home()}/.cache/sqlite/{db_name}"
    db_url = f"sqlite+pysqlite:///{db_path}"

    try:
        logging.info("Creating SQL engine")
        engine = create_engine(db_url, echo=echo)
    except Exception:
        logging.error("Failed to create SQL engine")
        raise

    logging.info("Successfully created SQL engine")
    return engine
