"""Utility functions for data modeling module"""

import pandas as pd


def extract_date_key(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset including the date columns deduplicated and a surrogate key"""

    df = df.copy()
    df["data_key"] = df["collision_datetime"].dt.strftime("%Y%m%d").astype("int")

    return df[["data_key"]]


def extract_time_key(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset including the surrogate key for the time dimension"""

    df = df.copy()
    df["hour"] = df["collision_datetime"].dt.hour
    df["minute"] = df["collision_datetime"].dt.minute
    # transform hour:minute to int: 0:1 -> 1; 08:15 -> 815; 23:59 -> 2359
    df["time_key"] = df["hour"] * 100 + df["minute"]

    return df[["time_key"]]


def extract_severity_key(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset including the surrogate key for the severity dimension"""

    df = df.copy()
    key_map = {"Slight": 1, "Serious": 2, "Fatal": 3}
    df["severity_key"] = df["collision_severity"].map(key_map)

    return df[["severity_key"]]


def extract_location_key(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset including the surrogate key for the location dimension"""

    df = df.copy()
    df["location_key"] = df.reset_index(drop=True).index + 1

    return df[["location_key"]]
