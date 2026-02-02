import logging

from de_project.common.config import get_data_path
from de_project.common.logging_config import setup_logging
from de_project.project_p5.ingest import ingest

from de_project.project_p5.spark import get_spark

setup_logging()
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting ETL pipeline (SPARK)")

    DATA_PATH = get_data_path() / "raw"

    spark = get_spark()

    # re-assign df variable to make some room
    path = DATA_PATH / "collision"
    df = ingest(spark, path)

    path = DATA_PATH / "vehicle"
    df = ingest(spark, path)

    path = DATA_PATH / "casualty"
    df = ingest(spark, path)

    spark.stop()
    logger.info("Successfully finished ETP pipeline (SPARK)")


if __name__ == "__main__":
    main()
