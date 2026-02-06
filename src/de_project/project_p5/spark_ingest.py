from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from pathlib import Path
import logging

from de_project.common.config import load_env
from de_project.project_p5.spark_schema import get_schema_for

load_env()
logger = logging.getLogger(__name__)


def _log_df_metrics(df: DataFrame, name: str) -> None:
    logger.info(
        "[METRIC] Extracted [%s] dataset shape : rows %s, cols %s",
        name,
        df.count(),
        len(df.columns),
    )
    logger.info(
        f"[METRIC] [%s] dataset partitions: %s", name, df.rdd.getNumPartitions()
    )


def ingest(spark: SparkSession, data_dir: Path) -> DataFrame:
    name = data_dir.name
    logger.info("Start datasets ingestion [%s]", name)

    df = spark.read.csv(
        path=str(data_dir),
        header=True,
        schema=get_schema_for(name),
    )
    _log_df_metrics(df, name)
    logger.info("Successfully ingested datasets [%s]", name)
    return df
