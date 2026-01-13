"""Builds a dataset representing location dimensional table"""

import pandas as pd


def build_dim_location(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset representing a location dimension in star schema"""

    BUCKET_PRECISION = 2  # grid size 1 square kilometer
    locations = (
        df[["longitude", "latitude"]]
        .assign(
            lon_bucket=lambda x: x["longitude"].round(BUCKET_PRECISION),
            lat_bucket=lambda x: x["latitude"].round(BUCKET_PRECISION),
        )
        .drop_duplicates(["lon_bucket", "lat_bucket"])
        .sort_values(["lon_bucket", "lat_bucket"])
        .reset_index(drop=True)
    )
    locations["location_key"] = locations.index + 1

    return locations[
        [
            "location_key",
            "lon_bucket",
            "lat_bucket",
        ]
    ]
