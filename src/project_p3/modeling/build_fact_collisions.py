"""Builds a dataset representing collisions fact table"""

import pandas as pd

from project_p3.utils.utils import (
    extract_date_key,
    extract_time_key,
    extract_severity_key,
    extract_location_key,
)


def build_fact_collisions(
    df: pd.DataFrame,
    dimensions: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Returns a dataset representing a collisions fact in star schema"""

    df = df.copy()

    # build dimensions datasets
    dim_date: pd.DataFrame = dimensions["dim_date"]
    dim_time: pd.DataFrame = dimensions["dim_time"]
    dim_severity: pd.DataFrame = dimensions["dim_severity"]
    dim_location: pd.DataFrame = dimensions["dim_location"]

    # derive surrogate keys
    df["date_key"] = extract_date_key(df)  # append to df, name date_key
    df["time_key"] = extract_time_key(df)  # append to df, name time_key
    df["severity_key"] = extract_severity_key(df)["severity_key"]
    df["location_key"] = extract_location_key(df)["location_key"]

    # add additional dimension columns
    df["collision_key"] = df.reset_index(drop=True).index + 1  # surrogate key
    df["collision_count"] = 1  # should be always 1

    # create
    (
        df.merge(right=dim_date, how="inner", on="date_key")
        .merge(right=dim_time, how="inner", on="time_key")
        .merge(right=dim_severity, how="inner", on="severity_key")
        .merge(right=dim_location, how="inner", on="location_key")
    )

    return df[
        [
            "collision_key",
            "collision_id",  # optional, keep it for traceability
            "date_key",
            "time_key",
            "severity_key",
            "location_key",
            "collision_count",
        ]
    ]
