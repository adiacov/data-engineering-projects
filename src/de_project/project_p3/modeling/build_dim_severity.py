"""Builds a dataset representing severity dimensional table"""

import pandas as pd

from de_project.project_p3.utils.utils import extract_severity_key


def _extract_severity_dedup(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset including the severity column deduplicated"""
    return df.drop_duplicates("collision_severity")


def build_dim_severity(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset representing a severity dimension in star schema"""

    df = df.copy()

    # derive dimension columns
    df["severity_key"] = extract_severity_key(df)
    df["severity_description"] = df["collision_severity"]
    group_map = {"Slight": "low", "Serious": "medium", "Fatal": "high"}
    df["severity_group"] = df["collision_severity"].map(group_map)

    # create dimension dataset
    df = _extract_severity_dedup(df)

    return df[
        [
            "severity_key",
            "severity_description",
            "severity_group",
        ]
    ]
