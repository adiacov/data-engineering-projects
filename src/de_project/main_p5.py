import logging

from de_project.common.config import get_data_path
from de_project.common.logging_config import setup_logging
from de_project.project_p5.ingest import ingest
from de_project.project_p5.transform.transform import transform_clean, transform_curate

from de_project.project_p5.spark import get_spark

setup_logging()
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting ETL pipeline (SPARK)")

    DATA_PATH = get_data_path() / "raw"

    spark = get_spark()

    # re-assign df variable to make some room
    ### COLLISION INGEST
    dataset_name = "collision"
    path = DATA_PATH / dataset_name
    df = ingest(spark, path)
    df.repartition(8)
    logger.info(
        "[METRIC] [%s] dataset partitions (after repartition): %s",
        dataset_name,
        df.rdd.getNumPartitions(),
    )

    ### COLLISION TRANSFORM
    df = transform_clean(df, dataset_name)
    df = transform_curate(df, dataset_name)

    # ### VEHICLE INGEST
    # dataset_name = "vehicle"
    # path = DATA_PATH / dataset_name
    # df = ingest(spark, path)
    # df.repartition(8)
    # logger.info(
    #     "[METRIC] [%s] dataset partitions (after repartition): %s",
    #     dataset_name,
    #     df.rdd.getNumPartitions(),
    # )

    # ### CASUALTY INGEST
    # dataset_name = "casualty"
    # path = DATA_PATH / dataset_name
    # df = ingest(spark, path)
    # df.repartition(8)
    # logger.info(
    #     "[METRIC] [%s] dataset partitions (after repartition): %s",
    #     dataset_name,
    #     df.rdd.getNumPartitions(),
    # )

    spark.stop()
    logger.info("Successfully finished ETL pipeline (SPARK)")


if __name__ == "__main__":
    main()
