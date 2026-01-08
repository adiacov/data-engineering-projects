"""Transformations for clean layer (raw dataset -> clean dataset)"""

import pandas as pd

from transform.column_mappings import get_mappings

import logging


# NOTE: !!! IMPORTANT
# Even if the DataFrame doesn't need some of transformations,
# I do those on purpose and for several reasons:
#   1. to have hands-on experience with pandas, python
#   2. to have a better understanding of processes while learning by doing
# One example of what I mean by this, is the columns names in the dataset.
# These already have a clear, all lower-case with underscore naming convention.
# So there is no need for me to do anything.

logger = logging.getLogger(__file__)


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a new dataset with renamed columns"""
    logger.info("- Start rename dataset columns...")

    df = (
        df.rename(columns=str.strip)
        .rename(columns=str.lower)
        .rename(columns={"collision_index": "collision_id"})
    )

    logger.info("- Finish renaming dataset columns")
    logger.info(
        f"[METRIC] Rename dataset columns: IN shape {df.shape} - OUT shape {df.shape}"
    )
    return df


def _cast_values(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a new dataset with cast data types"""
    logger.info("- Start cast dataset values")

    df = df.copy()
    # cast values: object to string
    obj_cols = df.select_dtypes(include="object").columns
    df[obj_cols] = df[obj_cols].astype("string")

    # cast values: int64 to int32
    int_cols = df.select_dtypes(include="int64").columns
    df[int_cols] = df[int_cols].astype("int32")

    # cast values: float64 to float 32
    float_cols = df.select_dtypes(include="float64").columns
    df[float_cols] = df[float_cols].astype("float32")

    logger.info("- Finish cast dataset columns")
    logger.info(
        f"[METRIC] Cast values data types - object to string: {obj_cols.to_list()}"
    )
    logger.info(
        f"[METRIC] Cast values data types -  int64 to int32: {int_cols.to_list()}"
    )
    logger.info(
        f"[METRIC] Cast values data types -  float64 to float32: {float_cols.to_list()}"
    )
    logger.info(
        f"[METRIC] Cast dataset values: IN shape {df.shape} - OUT shape {df.shape}"
    )
    return df


# Handle columns mapping
def _normalize_column_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize categorical code columns using predefined value mappings,
    failing on unmapped values."""

    df = df.copy()
    for col, mapping in get_mappings().items():
        if col not in df.columns:
            logger.warning(f"Trying to transform non-existing column: {col}")
            continue

        s = df[col]
        mapped = s.map(mapping)

        # validate coverage (ignore NaN in source)
        mask_unmapped = mapped.isna() & s.notna()
        if mask_unmapped.any():
            missing = s[mask_unmapped].unique()
            raise ValueError(
                f"Unmapped values in column {col} while transforming: {missing}"
            )

        df[col] = mapped

    return df


# Normalize special columns
def _normalize_int_special_codes(
    series: pd.Series, mapping: dict[int, str]
) -> pd.Series:
    """Returns the value in the mapping if exists
    or the unchanged value in the series cast to string"""

    result = series.astype("Int64").astype("string")
    for code, value in mapping.items():
        result = result.mask(series == code, value)

    if result.isna().any():
        raise ValueError(
            f"Introduced NaN values in column {series.name} after transformation"
        )

    return result


# Normalize column values
def _normalize_values(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a new dataset with normalized values"""
    logger.info("- Start normalize dataset values")

    # Map categorical values
    df = _normalize_column_codes(df)

    # Map categorical values special cases
    df["first_road_number"] = _normalize_int_special_codes(
        df["first_road_number"], {-1: "Unknown", 0: "Unclassified"}
    )
    df["speed_limit"] = _normalize_int_special_codes(
        df["speed_limit"], {-1: "Unknown", 99: "Unknown"}
    )
    df["second_road_number"] = _normalize_int_special_codes(
        df["second_road_number"], {-1: "Unknown", 0: "Unclassified"}
    )

    # Strip whitespace in column values
    str_cols = df.select_dtypes(include="string").columns
    df[str_cols] = df[str_cols].apply(lambda s: s.str.strip())

    logger.info("- Finish normalize dataset values")
    logger.info(
        f"[METRIC] Normalize dataset values: IN shape {df.shape} - OUT shape {df.shape}"
    )
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Returns a cleaned dataset by applying transformations and verification rules"""
    logger.info("Start cleaning dataset...")

    df = df.copy()
    df = _rename_columns(df)
    df = _normalize_values(df)
    df = _cast_values(df)

    logger.info("Successfully cleaned dataset")
    return df
