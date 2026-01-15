"""Builds a dataset representing time dimensional table"""

import pandas as pd

from project_p3.utils.utils import extract_time_key


def _extract_time_dedup(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset including the time columns deduplicated"""
    return df.drop_duplicates(["hour", "minute"]).sort_values(["hour", "minute"])


def build_dim_time(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset representing a time dimension in star schema"""

    df = df.copy()

    # derive dimension columns
    df["time_key"] = extract_time_key(df)
    df["hour"] = df["collision_datetime"].dt.hour
    df["minute"] = df["collision_datetime"].dt.minute

    # create dimension dataset
    df = _extract_time_dedup(df)

    return df[
        [
            "time_key",
            "hour",
            "minute",
        ]
    ]
