from pyspark.sql import DataFrame
import pyspark.sql.functions as fc
from pyspark.sql.types import IntegerType

import logging

logger = logging.getLogger(__name__)


def _filter_columns(df: DataFrame) -> DataFrame:
    logger.info("- Filter invalid rows")

    df = (
        df.filter((df.latitude >= -90) & (df.latitude <= 90))
        .filter((df.longitude >= -180) & (df.longitude <= 180))
        .dropna(subset="collision_index")
        .dropDuplicates(subset=["collision_index"])
    )

    return df


def _validate(df: DataFrame) -> DataFrame:
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

    df = df.alias("df_clean")
    df = _filter_columns(df)
    df = _validate(df)
    return df
