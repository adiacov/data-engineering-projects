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

import logging

from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def main_p2():
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
    pass


if __name__ == "__main__":
    main_p2()
