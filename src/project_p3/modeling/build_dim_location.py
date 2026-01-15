"""Builds a dataset representing location dimensional table"""

import pandas as pd

from project_p3.utils.utils import extract_location_key


def _extract_location_dedup(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset including the location columns deduplicated"""
    cols = ["lon_bucket", "lat_bucket"]
    return df.drop_duplicates(cols).sort_values(cols)


def build_dim_location(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset representing a location dimension in star schema"""

    df: pd.DataFrame = df.copy()

    # derive dimension columns
    BUCKET_PRECISION = 2  # grid size 1 square kilometer
    df["lon_bucket"] = df["longitude"].astype("float").round(BUCKET_PRECISION)
    df["lat_bucket"] = df["latitude"].astype("float").round(BUCKET_PRECISION)
    df["location_key"] = extract_location_key(df)

    # create dimension dataset
    df = _extract_location_dedup(df)

    return df[
        [
            "location_key",
            "lon_bucket",
            "lat_bucket",
        ]
    ]
