"""ETL load"""

import pandas as pd
from sqlalchemy import Engine, Connection, text

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


def _data_quality_check(con: Connection, df_row_count: int) -> None:
    """Validates the inserted database rows versus dataset rows"""

    logger.info("Performing data quality check...")
    select_duplicates = text(
        (
            "SELECT collision_index, COUNT(*) AS collision_index_count "
            "FROM collisions_raw "
            "GROUP BY collision_index "
            "HAVING COUNT(*) > 1;"
        )
    )

    count_all = text("SELECT COUNT(*) AS db_rows_count FROM collisions_raw;")

    try:
        sql_result = con.execute(select_duplicates)
        if len(sql_result.fetchall()) > 0:
            rows = [row for row in sql_result.fetchall()]
            logger.error("There are duplicate dataset rows inserted into DB")
            logger.error(f"Duplicate rows:\n{rows}")
            raise Exception(
                "Data quality check failed, because of duplicate rows inserted into DB"
            )

        sql_count_result = con.execute(count_all)

        if int(sql_count_result.first().db_rows_count) != df_row_count:
            logger.error("Rows count mismatch: database rows vs dataset rows")
            raise Exception(
                "Data quality check failed, because of rows count mismatch between database and dataset"
            )
        logger.info("Data quality check pass")
    except Exception:
        logger.error("Failed data quality check")
        raise


def load_data(df: pd.DataFrame, engine: Engine) -> None:
    """Loads the dataset to database"""
    logger.info("Loading dataset to database...")

    with engine.connect() as conn:
        try:
            df.to_sql(
                name="collisions_raw",
                con=conn,
                if_exists="replace",
                index=False,
            )

            _data_quality_check(con=conn, df_row_count=len(df))

            logger.info(f"Successfully loaded data into DB")
            conn.commit()
        except Exception:
            logger.error("Failed to load data into DB")
            raise
