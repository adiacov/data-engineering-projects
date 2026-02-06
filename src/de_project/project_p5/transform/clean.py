from pyspark.sql import DataFrame
import pyspark.sql.functions as fc
from pyspark.sql.types import IntegerType

import logging

logger = logging.getLogger(__name__)


# COLLISIONS
def _filter_columns_collision(df: DataFrame) -> DataFrame:
    logger.info("- Filter invalid rows")

    df = (
        df.filter((df.latitude >= -90) & (df.latitude <= 90))
        .filter((df.longitude >= -180) & (df.longitude <= 180))
        .dropna(subset="collision_index")
        .dropDuplicates(subset=["collision_index"])
    )

    return df


def _validate_collision(df: DataFrame) -> DataFrame:
    logger.info("- Validating cleaned dataset")

    year = fc.split_part(df.date, fc.lit("/"), fc.lit(-1)).cast(IntegerType())
    bad_condition = df.collision_index.isNull() | (df.collision_year != year)
    # is_invalid if the bad_condition column is True for any condition in the expression
    is_invalid = df.filter(bad_condition)

    if is_invalid.head(1):
        logger.error("Failed validation for cleaned dataset")
        raise Exception("Failed clean dataset validation.")

    return df


def clean_collision(df: DataFrame) -> DataFrame:
    """Returns a new dataset after cleaning transformation"""

    df_clean = df.alias("df_clean")
    df_clean = _filter_columns_collision(df_clean)
    df_clean = _validate_collision(df_clean)
    return df_clean


# VEHICLES
def _filter_columns_vehicle(df: DataFrame) -> DataFrame:
    logger.info("- Filter invalid rows")

    df = (
        df.dropna(subset="collision_index")
        .filter((df.age_of_driver >= 0) & (df.age_of_driver <= 150))
    )

    return df


def _validate_vehicle(df: DataFrame) -> DataFrame:
    logger.info("- Validating cleaned dataset")

    bad_condition = df.collision_index.isNull() | df.age_of_driver.isNull()
    # is_invalid if the bad_condition column is True for any condition in the expression
    is_invalid = df.filter(bad_condition)

    if is_invalid.head(1):
        logger.error("Failed validation for cleaned dataset")
        raise Exception("Failed clean dataset validation.")

    return df


def clean_vehicle(df: DataFrame) -> DataFrame:
    """Returns a new dataset after cleaning transformation"""
    df_clean = df.alias("df_clean")
    df_clean = _filter_columns_vehicle(df_clean)
    df_clean = _validate_vehicle(df_clean)
    return df_clean


# CASUALTIES
def _filter_columns_casualty(df: DataFrame) -> DataFrame:
    logger.info("- Filter invalid rows")

    df = (
        df.dropna(subset="collision_index")
        .filter((df.age_of_casualty >= 0) & (df.age_of_casualty <= 150))
    )

    return df


def _validate_casualty(df: DataFrame) -> DataFrame:
    logger.info("- Validating cleaned dataset")

    bad_condition = df.collision_index.isNull() | df.age_of_casualty.isNull()
    # is_invalid if the bad_condition column is True for any condition in the expression
    is_invalid = df.filter(bad_condition)

    if is_invalid.head(1):
        logger.error("Failed validation for cleaned dataset")
        raise Exception("Failed clean dataset validation.")

    return df


def clean_casualty(df: DataFrame) -> DataFrame:
    """Returns a new dataset after cleaning transformation"""
    df_clean = df.alias("df_clean")
    df_clean = _filter_columns_casualty(df_clean)
    df_clean = _validate_casualty(df_clean)
    return df_clean
