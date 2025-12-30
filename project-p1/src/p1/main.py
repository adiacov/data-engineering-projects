"""ETL pipeline - local file batch ingestion"""

from p1.extract import read_csv_file
from p1.load import load_csv_file, load_data
from p1.transform import transform

import logging

logger = logging.getLogger(__name__)


def main() -> None:
    """ETL pipeline
    - Read a local CSV file
    - Transform, Validate the data
    - Load the clean data info a CSV file
    - Load the data into a local SQLite DB
    """
    logger.info("ETL pipeline started")

    try:
        df = read_csv_file()
        df = transform(df)
        load_csv_file(df)  # intermediate state (debug, evolution comparison, etc.)
        load_data(df)  # data for downstream processes
        logger.info("ETL pipeline finished successfully")
    except Exception:
        logger.exception("ETL pipeline failed")
        raise


if __name__ == "__main__":
    main()
