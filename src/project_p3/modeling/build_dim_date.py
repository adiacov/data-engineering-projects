"""Builds a dataset representing date dimensional table"""

import pandas as pd
from hashlib import md5

import logging

logger = logging.getLogger(__name__)


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset representing a date dimension in star schema"""

    dates = (
        df["collision_datetime"]
        .dt.normalize()
        .drop_duplicates()
        .sort_values()
        .to_frame(name="date")
    )

    dates["date_key"] = dates["date"].dt.strftime("%Y%m%d").astype(int)
    dates["year"] = dates["date"].dt.year
    dates["month"] = dates["date"].dt.month
    dates["day"] = dates["date"].dt.day
    dates["day_of_week"] = dates["date"].dt.day_name()
    dates["is_weekend"] = dates["date"].dt.day_of_week >= 5

    return dates[
        [
            "date_key",
            "year",
            "month",
            "day",
            "day_of_week",
            "is_weekend",
        ]
    ]
