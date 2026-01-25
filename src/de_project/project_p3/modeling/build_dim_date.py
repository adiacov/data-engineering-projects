"""Builds a dataset representing date dimensional table"""

import pandas as pd

import logging

from de_project.project_p3.utils.utils import extract_date_key

logger = logging.getLogger(__name__)


def _extract_date_dedup(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset including the date column normalized and deduplicated"""
    return df.drop_duplicates("date").sort_values("date")


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset representing a date dimension in star schema"""

    df = df.copy()

    # derive dimension columns
    df["date_key"] = extract_date_key(df)
    df["date"] = df["collision_datetime"].dt.normalize()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["day"] = df["date"].dt.day
    df["day_of_week"] = df["date"].dt.day_name().astype("string")
    df["is_weekend"] = df["date"].dt.day_of_week >= 5

    # create dimension dataset
    df = _extract_date_dedup(df)

    return df[
        [
            "date_key",
            "date",
            "year",
            "month",
            "day",
            "day_of_week",
            "is_weekend",
        ]
    ]
