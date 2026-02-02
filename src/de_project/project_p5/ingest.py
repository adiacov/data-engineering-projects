from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from pathlib import Path
import logging

from de_project.common.logging_config import setup_logging
from de_project.common.config import load_env
from de_project.project_p5.schema import get_schema_for

load_env()
setup_logging()
logger = logging.getLogger(__name__)


def _log_df_summary(name: str, df: DataFrame) -> None:
    logger.info(f"Ingested dataset {name}")
    logger.info(f"- Dataset shape: rows - {df.count()}, cols: {len(df.columns)}")


def ingest(spark: SparkSession, data_dir: Path) -> DataFrame:
    name = data_dir.name
    logger.info("Start datasets ingestion [%s]", name)

    df = spark.read.csv(
        path=str(data_dir),
        header=True,
        schema=get_schema_for(name),
    )
    _log_df_summary(name, df)

    logger.info("Successfully ingested datasets [%s]", name)
    return df
