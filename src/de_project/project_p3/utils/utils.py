"""Utility functions for data modeling module"""

import pandas as pd

from typing import Iterable, Literal


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


from typing import Iterable, Literal


def validate_dataset_for_empty(df: pd.DataFrame) -> pd.DataFrame:
    """Checks the dataset for missing values.

    Returns:
        The same dataset after a successful validation

    Raises:
        ValueError if any of validation didn't pass
    """

    has_nulls = False
    null_cols = []

    for col in df.columns:
        if not df[col].isna().any():
            continue
        else:
            null_cols.append(col)
            has_nulls = True

    if has_nulls:
        raise ValueError(
            f"Validate dimension failed. Dataset contains missing values in columns {null_cols}."
        )

    return df


def validate_dataset_for_duplicate(
    df: pd.DataFrame,
    duplicate_cols: str | Iterable[str] | Literal["all"] = "all",
) -> pd.DataFrame:
    """Checks the dataset for duplicate values.

    :param:
        df - the dataset to validate
    :param:
        duplicate_cols - a subset of column names to check for duplicates
        Where:
        `str` - a single column name;
        `Iterable[str]` - a subset of column names;
        `Literal["all"]` - all columns in the dataset;
        `None` - default, don't check for duplicate columns;

    Returns:
        The same dataset after a successful validation

    Raises:
        ValueError if there are duplicate values in columns
    """

    has_duplicates = False
    duped_cols = []

    # check duplicates in all columns
    if duplicate_cols == "all":
        for col in df.columns:
            if not df[col].duplicated().any():
                continue
            else:
                duped_cols.append(col)
                has_duplicates = True

    # check duplicates for a single column
    elif isinstance(duplicate_cols, str):
        if df[duplicate_cols].duplicated().any():
            duped_cols.append(duplicate_cols)
            has_duplicates = True

    # check duplicates for a subset of columns
    elif isinstance(duplicate_cols, Iterable):
        for col in duplicate_cols:
            if not df[col].duplicated().any():
                continue
            else:
                duped_cols.append(col)
                has_duplicates = True

    if has_duplicates:
        raise ValueError(
            f"Validate dimension failed. Dataset contains duplicate values in columns {duped_cols}."
        )

    return df


def validate_dataset(
    df: pd.DataFrame,
    duplicate_cols: str | Iterable[str] | Literal["all"] = "all",
) -> pd.DataFrame:
    """Check dataset for null and duplicate columns.
    Fail fast if any error occurred.
    """
    validate_dataset_for_duplicate(validate_dataset_for_empty(df), duplicate_cols)
    return df
