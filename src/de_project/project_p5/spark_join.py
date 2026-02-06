from pyspark.sql import DataFrame
import pyspark.sql.functions as fc

import logging

logger = logging.getLogger(__name__)


def join_datasets(
    df_collision: DataFrame,
    df_vehicle: DataFrame,
    df_casualty: DataFrame,
) -> DataFrame:
    """Returns a joined dataset"""

    logger.info("Start joining datasets...")
    df_vehicle_cp = df_vehicle.drop("collision_year", "collision_ref_no")
    df_casualty_cp = df_casualty.drop(
        "collision_year",
        "collision_ref_no",
        "vehicle_reference",
    )

    df = df_collision.join(
        other=df_vehicle_cp,
        on="collision_index",
        how="inner",
    ).join(
        other=df_casualty_cp,
        on="collision_index",
        how="inner",
    )

    logger.info("Joined datasets.")
    logger.info(
        "[METRIC] Joined dataset shape: rows %s, cols %s", df.count(), len(df.columns)
    )

    return df
