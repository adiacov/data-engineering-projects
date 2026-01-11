"""Transformations for curated layer (clean dataset -> curated dataset)"""

import pandas as pd

import logging

logger = logging.getLogger(__name__)

# Curated transformations


def _curate_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a new dataset with derived data"""

    # Derive is weekend day
    weekend_days = ["Sunday", "Saturday"]
    df["is_weekend_day"] = (
        df["day_of_week"]
        .case_when(
            caselist=[
                (df["day_of_week"].isin(weekend_days), True),
                (~df["day_of_week"].isin(weekend_days), False),
            ]
        )
        .astype("bool")
    )

    # Derive collision hour:minutes
    df["collision_time"] = df["collision_datetime"].dt.strftime("%H:%M")
    df["collision_time"] = df["collision_time"].astype("string")

    # Derive collision year:month
    df["collision_year_month"] = df["collision_datetime"].dt.strftime("%Y-%m")
    df["collision_year_month"] = df["collision_year_month"].astype("string")

    # Derive severity group
    df["severity_group"] = (
        df["collision_severity"]
        .case_when(
            [
                (df["collision_severity"] == "Slight", "low"),
                (df["collision_severity"] == "Serious", "medium"),
                (df["collision_severity"] == "Fatal", "high"),
            ]
        )
        .astype("string")
    )

    return df


def curate(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a new dataframe with derived data for business needs"""

    logger.info("Start derive business data...")
    try:
        df = df.copy()
        df = _curate_dataset(df)
    except Exception:
        logger.error("Failed to derive business data")
        raise

    logger.info("Successfully derived business data")
    return df
