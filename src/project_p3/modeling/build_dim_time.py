"""Builds a dataset representing time dimensional table"""

import pandas as pd


def build_dim_time(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset representing a time dimension in star schema"""

    times = df["collision_datetime"]
    times = (
        pd.DataFrame(
            {
                "hour": times.dt.hour,
                "minute": times.dt.minute,
            },
        )
        .drop_duplicates()
        .sort_values(["hour", "minute"])
    )

    # 0:1 -> 1; 08:15 -> 815; 23:59 -> 2359
    times["time_key"] = times["hour"] * 100 + times["minute"]

    return times[
        [
            "time_key",
            "hour",
            "minute",
        ]
    ]
