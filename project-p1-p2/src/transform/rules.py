"""Data quality check rules"""

import pandas as pd

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def _apply_not_null_rule(
    df: pd.DataFrame,
    subset: list[str],
) -> tuple[pd.DataFrame, str]:
    """Applies null quality check rule.

    Returns a two elements tuple:
    - first: the filtered dataset without rows containing null values
    - second: a metric describing the number of affected rows
    """

    df = df.copy()
    # mask: False = row to keep
    mask = df[subset].isna().any(axis=1)
    removed = (mask).sum()
    df = df[~mask]

    message = (
        f"{removed} rows where removed after 'null' quality check for columns {subset}"
    )

    return (df, message)


def _apply_not_duplicate_rule(
    df: pd.DataFrame,
    subset: list[str],
    keep="last",
) -> tuple[pd.DataFrame, str]:
    """Applies duplicate quality check rule.

    Returns a two elements tuple:
    - first: the filtered dataset without rows containing duplicate values
    - second: a metric describing the number of affected rows
    """

    df = df.copy()
    # mask: False = keep row
    mask = df.duplicated(subset=subset, keep=keep)
    removed = (mask).sum()
    df = df[~mask]
    message = f"{removed} rows where removed after 'duplicate' quality check for columns {subset}"

    return (df, message)


def _apply_in_range_rule(
    df: pd.DataFrame,
    col: str,
    min: int | float | datetime,
    max: int | float | datetime,
) -> tuple[pd.DataFrame, str]:
    """Applies range quality check rule.

    Returns a two elements tuple:
    - first: the filtered dataset with rows fixed or dropped outside range
    - second: a metric describing the number of affected rows
    """

    df = df.copy()
    # mask: False = keep row
    mask = ~((df[col] >= min) & (df[col] <= max))
    removed = (mask).sum()
    df = df[~mask]
    message = (
        f"{removed} rows where removed after 'in range' quality check for column {col}"
    )

    return (df, message)


def quality_check(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a validated dataset after applying quality check rules"""
    df = df.copy()

    # apply not null rule
    cols = ["collision_id"]
    df, message = _apply_not_null_rule(df, cols)
    logger.info(f"[METRIC] {message}")

    # apply not duplicate rule
    cols = ["collision_id"]
    df, message = _apply_not_duplicate_rule(df, cols)
    logger.info(f"[METRIC] {message}")

    # apply range rule
    col = "latitude"
    df, message = _apply_in_range_rule(df, col, -90, 90)
    logger.info(f"[METRIC] {message}")

    # apply range rule
    col = "longitude"
    df, message = _apply_in_range_rule(df, col, -180, 180)
    logger.info(f"[METRIC] {message}")

    col = "collision_datetime"
    jan_1_2023 = datetime(2023, 1, 1)
    jan_1_2024 = datetime(2024, 1, 1)
    df, message = _apply_in_range_rule(df, col, jan_1_2023, jan_1_2024)
    logger.info(f"[METRIC] {message}")

    return df
