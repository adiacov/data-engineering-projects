"""Builds a dataset representing severity dimensional table"""

import pandas as pd


def build_dim_severity(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset representing a severity dimension in star schema"""

    severity = (
        df["collision_severity"].drop_duplicates().to_frame(name="severity_description")
    )

    key_map = {"Slight": 1, "Serious": 2, "Fatal": 3}
    group_map = {"Slight": "low", "Serious": "medium", "Fatal": "high"}

    severity["severity_key"] = severity["severity_description"].map(key_map)
    severity["severity_group"] = severity["severity_description"].map(group_map)

    return severity[
        [
            "severity_key",
            "severity_description",
            "severity_group",
        ]
    ]
