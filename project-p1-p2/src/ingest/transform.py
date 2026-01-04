"""Transfomation ETL step"""

import pandas as pd
import pandera.pandas as pa

from ingest.schema import CollisionsRawSchema
import logging

logger = logging.getLogger(__name__)


@pa.check_output(
    schema=CollisionsRawSchema.to_schema(),
    lazy=True,
)
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a transformed and validated dataset"""
    logger.info("Starting dataset transformation...")

    try:
        transformed_df = _transform(df)
        logger.info("Finished dataset transformation")
        logger.info("Successfully validated the dataset")
        return transformed_df
    except Exception:
        logger.error("Could not transform the dataset")
        raise


def _transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transform dataset"""

    # Convert date type from string to datetime
    if {"date", "time"}.issubset(df.columns):
        dt_str = df["date"].astype(str) + "T" + df["time"].astype(str)
        df["collision_datetime"] = pd.to_datetime(
            arg=dt_str,
            format="%d/%m/%YT%H:%M",
            cache=True,
        )
        logger.info(f"- Created new column collision_datetime")

    # Drop rows containing null values
    df = df.dropna()
    logger.info("- Removed rows containing null values")

    # Drop duplicate rows
    duplicate_columns = ["collision_index"]
    df = df.drop_duplicates(duplicate_columns)
    logger.info(f"- Removed duplicate rows by columns: {duplicate_columns}")

    # Drop unnecessary columns
    drop_columns = ["date", "time"]
    df = df.drop(drop_columns, axis=1)
    logger.info(f"- Removed unnecessary columns: {drop_columns}")

    return df
