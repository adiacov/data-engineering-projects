from pyspark.sql import DataFrame, Column
import pyspark.sql.functions as fc
from pyspark.sql.types import Dict

### LOCATION DIM


def derive_location_key() -> Column:
    """Derive surrogate key for location dimension table"""
    return fc.hash("lon_bucket", "lat_bucket")


def derive_location_buckets() -> Dict[str, Column]:
    """Creates a dictionary with location buckets for a certain scale.

    Rounding measures:
    - round(..., 3) = ~100 m precision
    - round(..., 2) = ~1 km precision
    """
    SCALE = 2
    return {
        "lon_bucket": fc.round(fc.col("longitude"), SCALE),
        "lat_bucket": fc.round(fc.col("latitude"), SCALE),
    }


def build_location_dimension(dff: DataFrame) -> DataFrame:
    """Returns a new dataset for location dimension table"""
    return (
        dff.dropna(subset=["longitude", "latitude"])
        .withColumns(derive_location_buckets())
        .dropDuplicates(["lon_bucket", "lat_bucket"])
        .withColumn("location_key", derive_location_key())
        .select(
            "location_key",
            "lon_bucket",
            "lat_bucket",
        )
    )


### DATE DIM


def derive_date_key() -> Column:
    """Derives date_key column as integer from date column.

    date_key = date col format("yyyyMMdd") -> 20220513 -> int
    format pattern: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    """
    return fc.date_format("date", "yyyyMMdd")


def build_date_dimension(dff: DataFrame) -> DataFrame:
    return (
        dff.withColumn("date_key", derive_date_key().cast("int"))
        .withColumn("year", fc.col("collision_year"))
        .withColumn("month", fc.month("date"))
        .withColumn("day", fc.day("date"))
        .withColumn("is_weekend", fc.weekday("date").isin([5, 6]))
        .select(
            "date_key",
            "date",
            "year",
            "month",
            "day",
            "day_of_week",
            "is_weekend",
        )
        .dropDuplicates(["date_key"])
    )


### TIME DIM


def derive_time_key() -> Column:
    """Derives time key as int from time column"""
    # transform hour:minute to int: 0:1 -> 1; 08:15 -> 815; 23:59 -> 2359
    return fc.regexp_replace("time", ":", "").cast("int")


def build_time_dim(dff: DataFrame) -> DataFrame:
    """Creates a dataset for time dimension table"""
    return (
        dff.withColumn("time_key", derive_time_key())
        .withColumn("hour", fc.split("time", ":", 2)[0].cast("int"))
        .withColumn("minute", fc.split("time", ":", 2)[1].cast("int"))
        .select(
            "time_key",
            "hour",
            "minute",
        )
        .dropDuplicates(["time_key"])
    )


### SEVERITY DIM


def derive_severity_key() -> Column:
    """Derives severity key"""

    return (
        fc.when(fc.col("collision_severity") == "Slight", 1)
        .when(fc.col("collision_severity") == "Serious", 2)
        .when(fc.col("collision_severity") == "Fatal", 3)
        .otherwise(None)
        .alias("severity_key")
    )


def derive_severity_group() -> Column:
    """Derives severity group"""

    return (
        fc.when(fc.col("collision_severity") == "Slight", "low")
        .when(fc.col("collision_severity") == "Serious", "medium")
        .when(fc.col("collision_severity") == "Fatal", "high")
        .otherwise(None)
        .alias("severity_key")
    )


def build_severity_dim(dff: DataFrame) -> DataFrame:
    """Creates a dataset for severity dimension table"""

    return (
        dff.withColumn("severity_key", derive_severity_key())
        .withColumn("severity_group", derive_severity_group())
        .select(
            "severity_key",
            fc.col("collision_severity").alias("severity_description"),
            "severity_group",
        )
        .dropDuplicates(["severity_key"])
    )


### FACT TABLE


def build_collisions_fact(dff: DataFrame) -> DataFrame:
    """Creates a dataset for collisions fact table"""

    return (
        dff.withColumnRenamed("collision_index", "collision_id")
        .withColumns(derive_location_buckets())
        .withColumn("location_key", derive_location_key())
        .withColumn("date_key", derive_date_key())
        .withColumn("time_key", derive_time_key())
        .withColumn("severity_key", derive_severity_key())
        .withColumn("collision_count", fc.lit(1))
        .select(
            "collision_id",
            "location_key",
            "date_key",
            "time_key",
            "severity_key",
            "collision_count",
        )
        .dropDuplicates(["collision_id"])
    )
