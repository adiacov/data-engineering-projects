from pyspark.sql import DataFrame

import logging

from de_project.project_p5.transform.clean_collision import clean_collision
from de_project.project_p5.transform.curate_collision import curate_collision

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

    df = df.alias("df_transform")
    match name:
        case "collision":
            result = clean_collision(df)
        case _:
            logger.error("Cannot transform dataset. Unhandled dataset [%s]", name)
            raise ValueError(
                "Dataset transformation failed. Unhandled dataset name: [%s]", name
            )

    _log_df_metrics(df, name)
    logger.info("Finished clean transformation for dataset [%s]", name)
    return result


def transform_curate(df: DataFrame, name: str) -> DataFrame:
    logger.info("Start curated transformation for dataset [%s]", name)

    df = df.alias("df_curate")
    match name:
        case "collision":
            result = curate_collision(df)
        case _:
            logger.error("Cannot transform dataset. Unhandled dataset [%s]", name)
            raise ValueError(
                "Dataset transformation failed. Unhandled dataset name: [%s]", name
            )

    _log_df_metrics(df, name)
    logger.info("Finished curated transformation for dataset [%s]", name)
    return result
