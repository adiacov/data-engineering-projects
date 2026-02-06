from pyspark.sql import DataFrame

import logging

from de_project.project_p5.transform.clean import (
    clean_collision,
    clean_vehicle,
    clean_casualty,
)
from de_project.project_p5.transform.curate import (
    curate_collision,
    curate_vehicle,
    curate_casualty,
)

logger = logging.getLogger(__name__)


def _log_df_metrics(df: DataFrame, name: str) -> None:
    logger.info(
        "[METRIC] Transformed [%s] dataset shape : rows %s, cols %s",
        name,
        df.count(),
        len(df.columns),
    )


def transform_clean(df: DataFrame, name: str) -> DataFrame:
    logger.info("Start clean transformation for dataset [%s]", name)

    result = df.alias("df_clean")
    match name:
        case "collision":
            result = clean_collision(df)
        case "vehicle":
            result = clean_vehicle(df)
        case "casualty":
            result = clean_casualty(df)
        case _:
            logger.error("Cannot transform dataset. Unhandled dataset [%s]", name)
            raise ValueError(
                "Dataset transformation failed. Unhandled dataset name: [%s]", name
            )

    _log_df_metrics(result, name)
    logger.info("Finished clean transformation for dataset [%s]", name)
    return result


def transform_curate(df: DataFrame, name: str) -> DataFrame:
    logger.info("Start curated transformation for dataset [%s]", name)

    result = df.alias("df_curate")
    match name:
        case "collision":
            result = curate_collision(df)
        case "vehicle":
            result = curate_vehicle(df)
        case "casualty":
            result = curate_casualty(df)
        case _:
            logger.error("Cannot transform dataset. Unhandled dataset [%s]", name)
            raise ValueError(
                "Dataset transformation failed. Unhandled dataset name: [%s]", name
            )

    _log_df_metrics(result, name)
    logger.info("Finished curated transformation for dataset [%s]", name)
    return result
