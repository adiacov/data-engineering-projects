"""Transformations for clean layer (raw dataset -> clean dataset)"""

from pandas import DataFrame

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


def _rename_columns(df: DataFrame) -> DataFrame:
    """Returns a new dataset with renamed columns"""
    logger.info("- Start rename dataset columns...")

    dff = (
        df.rename(columns=str.strip)
        .rename(columns=str.lower)
        .rename(columns={"collision_index": "collision_id"})
    )

    logger.info("- Finish renaming dataset columns")
    logger.info(
        f"[METRIC] Rename dataset columns: IN shape {df.shape} - OUT shape {dff.shape}"
    )
    return dff


def _cast_columns(df: DataFrame) -> DataFrame:
    """Returns a new dataset with casted data types"""
    logger.info("- Start cast dataset columns")

    dff = df
    # cast columns: object to string
    obj_cols = dff.select_dtypes(include="object").columns
    dff[obj_cols] = dff[obj_cols].astype("string")

    # cast columns: int64 to int32
    int_cols = dff.select_dtypes(include="int64").columns
    dff[int_cols] = dff[int_cols].astype("int32")

    # cast columns: float64 to float 32
    float_cols = dff.select_dtypes(include="float64").columns
    dff[float_cols] = dff[float_cols].astype("float32")

    logger.info("- Finish cast dataset columns")
    logger.info(f"[METRIC] Cast object to string columns: {obj_cols.to_list()}")
    logger.info(f"[METRIC] Cast int64 to int32 columns: {int_cols.to_list()}")
    logger.info(f"[METRIC] Cast float64 to float32 columns: {float_cols.to_list()}")
    logger.info(
        f"[METRIC] Cast dataset columns: IN shape {df.shape} - OUT shape {dff.shape}"
    )
    return dff


def _normalize_values(df: DataFrame) -> DataFrame:
    """Returns a new dataset with normalized values"""
    logger.info("- Start normalize dataset values")

    dff = df

    # Strip whitespace in column values
    str_cols = dff.select_dtypes(include="string").columns
    dff[str_cols] = dff[str_cols].apply(lambda s: s.str.strip())

    # TODO - CONTINUE HERE

    logger.info("- Finish normalize dataset values")
    logger.info(
        f"[METRIC] Normalize dataset values: IN shape {df.shape} - OUT shape {dff.shape}"
    )
    return dff


def collisions_clean(df: DataFrame) -> DataFrame:
    """Returns a cleaned dataset by applying transformations and verification rules"""
    logger.info("Start cleaning dataset...")

    dff = df.copy()
    dff = _rename_columns(dff)
    dff = _normalize_values(dff)
    dff = _cast_columns(dff)

    logger.info("Successfully cleaned dataset")
    return dff
