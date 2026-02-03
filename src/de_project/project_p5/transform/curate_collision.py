from pyspark.sql import DataFrame, Column
import pyspark.sql.functions as fc
from pyspark.sql.types import IntegerType

import logging

logger = logging.getLogger(__name__)


def _derive_day_of_week(col: Column) -> Column:
    return (
        fc.when(col == 1, "Sunday")
        .when(col == 2, "Monday")
        .when(col == 3, "Tuesday")
        .when(col == 4, "Wednesday")
        .when(col == 5, "Thursday")
        .when(col == 6, "Friday")
        .when(col == 7, "Saturday")
        .otherwise(None)
    )


def _derive_collision_severity(col: Column) -> Column:
    return (
        fc.when(col == 1, "Fatal")
        .when(col == 2, "Serious")
        .when(col == 3, "Slight")
        .otherwise(None)
    )


def _derive_columns(df: DataFrame) -> DataFrame:
    logger.info("- Deriving new columns")

    # NOTE: This step could derive many more columns.
    # I already implemented it fully in project_p1_p2 using pandas.
    # Since this part is purely technical and not very insightful, it is shorter here
    # to focus time on more interesting aspects of the project.

    hour_col = fc.element_at(fc.split(df.time, ":"), 1).cast(IntegerType())
    date_col = fc.to_date(df.date, "dd/MM/yyyy")

    df = (
        df.withColumn("collision_hour", hour_col)
        .withColumn("date", date_col)
        .withColumn("day_of_week", _derive_day_of_week(df.day_of_week))
        .withColumn(
            "collision_severity", _derive_collision_severity(df.collision_severity)
        )
    )

    return df


def _validate(df: DataFrame) -> DataFrame:
    logger.info("- Validating curated dataset")

    bad_condition = (
        df.day_of_week.isNull()
        | df.collision_severity.isNull()
        | df.collision_hour.isNull()
        | df.date.isNull()
    )
    # is_invalid if the bad_condition column is True for any condition in the expression
    is_invalid = df.filter(bad_condition)

    if is_invalid.head(1):
        logger.error("Failed validation for cleaned dataset")
        raise Exception("Failed clean dataset validation.")

    return df


def curate_collision(df: DataFrame) -> DataFrame:
    """Returns a new dataset after curated transformation"""

    df = df.alias("df_curate")
    df = _derive_columns(df)
    df = _validate(df)
    return df
