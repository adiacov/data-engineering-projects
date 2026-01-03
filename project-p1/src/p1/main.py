"""ETL pipeline - local file batch ingestion"""

from p1.extract import read_csv_file
from p1.load import load_csv_file, load_data
from p1.transform import transform
from p1.db import create_db_engine
from p1.metadata import (
    create_ingestion_medatada,
    create_metadata_table,
    get_ingested_metadata,
    load_metadata,
    validate_metadata,
)

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def main() -> None:
    """ETL pipeline
    - Read a local CSV file
    - Transform, Validate the data
    - Load the clean data info a CSV file
    - Load the data into a local SQLite DB
    """

    BASE_PATH = Path(__file__).resolve().parents[2]

    RAW_DATA_FILE = "dft-road-casualty-statistics-collision-2023.csv"
    RAW_DATA_FILE_PATH = BASE_PATH / "data" / "raw" / RAW_DATA_FILE

    CLEAN_DATA_FILE = "dft-road-casualty-statistics-collision-2023-clean.csv"
    CLEAN_DATA_FILE_PATH = BASE_PATH / "data" / "processed" / CLEAN_DATA_FILE

    logger.info("ETL pipeline started")

    # Note: In real project the extract step would do some kind of API call to get the raw data.
    # Here I've put already the raw data in '/data/raw' directory.
    # I don't make a new call on each pipeline run, to save resources.
    try:
        engine = create_db_engine()

        new_metadata = create_ingestion_medatada(RAW_DATA_FILE_PATH)

        # create new metadata table if not exists. ensures the next step won't fail because of missing table.
        create_metadata_table(engine)
        current_metadata = get_ingested_metadata(engine, RAW_DATA_FILE_PATH)
        (valid_metadata, need_replacement) = validate_metadata(
            new_metadata, current_metadata
        )

        if valid_metadata and need_replacement:
            raw_df = read_csv_file(RAW_DATA_FILE_PATH)
            df = transform(raw_df)
            load_csv_file(CLEAN_DATA_FILE_PATH, df)
            load_data(df, engine)
            load_metadata(engine, new_metadata)
            logger.info("ETL pipeline finished successfully")
        elif valid_metadata and not need_replacement:
            logger.info(
                f"Skip ETL pipeline. The dataset already exists {valid_metadata}"
            )
        else:
            logger.error("Failed to validate ingestion metadata")
            raise Exception("Invalid ingestion metadata")
    except Exception:
        logger.exception("ETL pipeline failed")
        raise


if __name__ == "__main__":
    main()
