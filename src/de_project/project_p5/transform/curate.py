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


def _derive_columns_collision(df: DataFrame) -> DataFrame:
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


def _validate_collision(df: DataFrame) -> DataFrame:
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

    df_curate = df.alias("df_curate")
    df_curate = _derive_columns_collision(df_curate)
    df_curate = _validate_collision(df_curate)
    return df_curate


# VEHICLE
def _derive_vehicle_type_col(col: Column) -> Column:
    return (
        fc.when(col == 1, "Pedal cycle")
        .when(col == 2, "Motorcycle 50cc and under")
        .when(col == 3, "Motorcycle 125cc and under")
        .when(col == 4, "Motorcycle over 125cc and up to 500cc")
        .when(col == 5, "Motorcycle over 500cc")
        .when(col == 8, "Taxi/Private hire car")
        .when(col == 9, "Car")
        .when(col == 10, "Minibus (8 - 16 passenger seats)")
        .when(col == 11, "Bus or coach (17 or more pass seats)")
        .when(col == 16, "Ridden horse")
        .when(col == 17, "Agricultural vehicle")
        .when(col == 18, "Tram")
        .when(col == 19, "Van / Goods 3.5 tonnes mgw or under")
        .when(col == 20, "Goods over 3.5t. and under 7.5t")
        .when(col == 21, "Goods 7.5 tonnes mgw and over")
        .when(col == 22, "Mobility scooter")
        .when(col == 23, "Electric motorcycle")
        .when(col == 90, "Other vehicle")
        .when(col == 97, "Motorcycle - unknown cc")
        .when(col == 98, "Goods vehicle - unknown weight")
        .when(col == 99, "Unknown vehicle type (self rep only)")
        .when(col == 103, "Motorcycle - Scooter (1979-1998)")
        .when(col == 104, "Motorcycle (1979-1998)")
        .when(col == 105, "Motorcycle - Combination (1979-1998)")
        .when(col == 106, "Motorcycle over 125cc (1999-2004)")
        .when(col == 108, "Taxi (excluding private hire cars) (1979-2004)")
        .when(col == 109, "Car (including private hire cars) (1979-2004)")
        .when(col == 110, "Minibus/Motor caravan (1979-1998)")
        .when(col == 113, "Goods over 3.5 tonnes (1979-1998)")
        .when(col == -1, "Unknown")
        .otherwise(None)
    )


def _derive_towing_and_articulation_col(col: Column) -> Column:

    return (
        fc.when(col == 0, "No tow/articulation")
        .when(col == 1, "Articulated vehicle")
        .when(col == 2, "Double or multiple trailer")
        .when(col == 3, "Caravan")
        .when(col == 4, "Single trailer")
        .when(col == 5, "Other tow")
        .when(col == 9, "Unknown (self reported)")
        .when(col == -1, "Unknown")
        .otherwise("Unknown")
    )


def _derive_columns_vehicle(df: DataFrame) -> DataFrame:
    logger.info("- Deriving new columns")

    # NOTE: This step could derive many more columns.
    # I already implemented it fully in project_p1_p2 using pandas.
    # Since this part is purely technical and not very insightful, it is shorter here
    # to focus time on more interesting aspects of the project.

    df = df.withColumn(
        "vehicle_type", _derive_vehicle_type_col(df.vehicle_type)
    ).withColumn(
        "towing_and_articulation",
        _derive_towing_and_articulation_col(df.towing_and_articulation),
    )

    return df


def _validate_vehicle(df: DataFrame) -> DataFrame:
    logger.info("- Validating curated dataset")

    bad_condition = df.vehicle_type.isNull() | df.towing_and_articulation.isNull()
    # is_invalid if the bad_condition column is True for any condition in the expression
    is_invalid = df.filter(bad_condition)

    if is_invalid.head(1):
        logger.error("Failed validation for cleaned dataset")
        raise Exception("Failed clean dataset validation.")

    return df


def curate_vehicle(df: DataFrame) -> DataFrame:
    """Returns a new dataset after curated transformation"""

    df_curate = df.alias("df_curate")
    df_curate = _derive_columns_vehicle(df_curate)
    df = _validate_vehicle(df)
    return df_curate


# CASUALTY
def _derive_casualty_class_col(col: Column) -> Column:
    return (
        fc.when(col == 1, "Driver or rider")
        .when(col == 2, "Passenger")
        .when(col == 3, "Pedestrian")
        .otherwise("Unknown")
    )


def _derive_casualty_severity_col(col: Column) -> Column:
    return (
        fc.when(col == 1, "Fatal")
        .when(col == 2, "Serious")
        .when(col == 3, "Slight")
        .otherwise("Unknown")
    )


def _derive_enhanced_casualty_severity_col(col: Column) -> Column:
    return (
        fc.when(col == 1, "Fatal")
        .when(col == 5, "Very Serious")
        .when(col == 6, "Moderately Serious")
        .when(col == 7, "Less Serious")
        .when(col == 3, "Slight")
        .when(col == -1, "Data missing or out of range")
        .otherwise("Unknown")
    )


def _derive_columns_casualty(df: DataFrame) -> DataFrame:
    logger.info("- Deriving new columns")

    # NOTE: This step could derive many more columns.
    # I already implemented it fully in project_p1_p2 using pandas.
    # Since this part is purely technical and not very insightful, it is shorter here
    # to focus time on more interesting aspects of the project.

    df = (
        df.withColumn("casualty_class", _derive_casualty_class_col(df.casualty_class))
        .withColumn(
            "casualty_severity", _derive_casualty_severity_col(df.casualty_severity)
        )
        .withColumn(
            "enhanced_casualty_severity",
            _derive_enhanced_casualty_severity_col(df.enhanced_casualty_severity),
        )
    )

    return df


def _validate_casualty(df: DataFrame) -> DataFrame:
    logger.info("- Validating curated dataset")

    bad_condition = (
        df.casualty_class.isNull()
        | df.casualty_severity.isNull()
        | df.enhanced_casualty_severity.isNull()
    )
    # is_invalid if the bad_condition column is True for any condition in the expression
    is_invalid = df.filter(bad_condition)

    if is_invalid.head(1):
        logger.error("Failed validation for cleaned dataset")
        raise Exception("Failed clean dataset validation.")

    return df


def curate_casualty(df: DataFrame) -> DataFrame:
    """Returns a new dataset after curated transformation"""

    df_curate = df.alias("df_curate")
    df_curate = _derive_columns_casualty(df_curate)
    df_curate = _validate_casualty(df_curate)
    return df_curate
