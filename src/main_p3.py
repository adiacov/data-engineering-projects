"""Main entry point to analytical data modeling"""

# 0. define collision grain:
# collision grain (raw definition, see final definition below) -> a single collision, on a certain date and time, on a location and with a severity
# collision grain unique? date (0..n) + time (0..n) + location (0..n) + severity (0..n) still not unique
# collision grain definition: a single collision happened per day and / or time, in a certain location, with a certain severity
# collision grain definition meaning: the fact table includes date/time/location/severity
# collision grain to be valid should have the following dimensions tables: date, time, location, severity
# collision grain should answer to questions like:
#   how many collisions per date and / or time, weekday or weekend, what is the severity level, where it happens most

# dimension + fact tables granularity
# fact is one collision line
# dim_date: yyyymmdd - has a granularity at the day level
# dim_time: hh:mm - has a granularity at the minute level
# dim_severity: describes the severity levels - granularity ??? is granularity applicable here?
# dim_location: latitude, longitude, geo_bucket - has a granularity at the lat/lon level

# TODO: implement the following flow (review after)
# 1. read collisions_curated
# 2. do your modeling thing
# 3. validate all : not_null, unique (choose the right place to do it)
# 4. create a dwh db schema
# 5. insert tables into dwh
# 6. run your queries -> log to terminal: Q/A manner


import pandas as pd

import logging
from logging_config import setup_logging

from common.db import create_db_engine
from project_p3.modeling import (
    build_dim_date,
    build_dim_time,
    build_dim_severity,
    build_dim_location,
    build_fact_collisions,
)
from project_p3.utils import utils

setup_logging()
logger = logging.getLogger(__name__)


def main_p3():
    """Pipeline orchestration script that:

    - Reads dataset from collisions_raw table
    - Creates dimension datasets
    - Validates dimension datasets
    - Loads dimension datasets into DB
    - Reads dimension datasets from DB
    - Creates fact dataset
    - Validates fact dataset
    - Loads fact dataset
    - Creates fact dataset
    - Loads fact dataset into DB
    """

    engine = create_db_engine(echo=False)

    logger.info("Starting create fact and dimensions tables")
    try:
        with engine.connect() as conn:
            logger.info("- Reading collisions_curated dataset from database")
            df = pd.read_sql_table("collisions_curated", conn)

            logger.info("- Creating dimension datasets")
            dim_date = utils.validate_dataset(
                build_dim_date.build_dim_date(df), "date_key"
            )
            dim_time = utils.validate_dataset(
                build_dim_time.build_dim_time(df), "time_key"
            )
            dim_severity = utils.validate_dataset(
                build_dim_severity.build_dim_severity(df), "severity_key"
            )
            dim_location = utils.validate_dataset(
                build_dim_location.build_dim_location(df), "location_key"
            )

            logger.info("- Writing dimension dataset into database")
            dim_date.to_sql(
                "collisions_dim_date",
                conn,
                if_exists="replace",
                index=False,
            )

            dim_time.to_sql(
                "collisions_dim_time",
                conn,
                if_exists="replace",
                index=False,
            )

            dim_severity.to_sql(
                "collisions_dim_severity",
                conn,
                if_exists="replace",
                index=False,
            )

            dim_location.to_sql(
                "collisions_dim_location",
                conn,
                if_exists="replace",
                index=False,
            )

            logger.info("- Reading dimension tables from database")
            dimensions = {}
            dimensions["dim_date"] = pd.read_sql_table("collisions_dim_date", conn)
            dimensions["dim_time"] = pd.read_sql_table("collisions_dim_time", conn)
            dimensions["dim_severity"] = pd.read_sql_table(
                "collisions_dim_severity", conn
            )
            dimensions["dim_location"] = pd.read_sql_table(
                "collisions_dim_location", conn
            )

            logger.info("- Creating fact dataset")
            fact_df = build_fact_collisions.build_fact_collisions(df, dimensions)
            fact_df = utils.validate_dataset(fact_df, "collision_key")

            logger.info("- Writing fact dataset into database")
            fact_df.to_sql(
                "collisions_fact",
                conn,
                if_exists="replace",
                index=False,
            )

            logger.info("Successfully created fact and dimension tables")

    except Exception:
        logger.error("Failed to create fact and dimensions tables")
        raise


if __name__ == "__main__":
    main_p3()
