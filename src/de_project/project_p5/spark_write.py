from pyspark.sql import DataFrame

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def write_to_parquet(
    df: DataFrame,
    target_path: Path,
) -> None:
    """Writes dataframe to a parquet file at target destination, partitioned by year"""

    logger.info("Saving dataset to %s:", str(target_path))

    part = "collision_year"

    df.write.parquet(
        path=str(target_path / part),
        partitionBy=part,
        mode="overwrite",
    )
    logger.info("Successfully saved dataset to parquet file.")
