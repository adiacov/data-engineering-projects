"""Builds a dataset representing collisions fact table"""

import pandas as pd


def build_fact_collisions(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a dataset representing a collisions fact in star schema"""

    # fact_collision_id -> surrogate key, PK
    # collision_id -> optional, original from curated, for traceability
    # date_key FK
    # time_key FK
    # severity_key FK
    # location_key FK
    # collisions_count INT -> always 1 (doesn't make sense, but don't forget, I'm learning now, so this is made up and it's a start point)

    pass
