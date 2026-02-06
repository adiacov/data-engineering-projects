import logging

from de_project.project_p5.spark import get_spark
from de_project.common.config import get_data_path
from de_project.common.logging_config import setup_logging
from de_project.project_p5.ingest import ingest
from de_project.project_p5.transform.transform import transform_clean, transform_curate
from de_project.project_p5.join import join_datasets


setup_logging()
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting ETL pipeline (SPARK)")

    DATA_PATH = get_data_path() / "raw"

    spark = get_spark()

    ### COLLISION INGEST
    dataset_name = "collision"
    path = DATA_PATH / dataset_name
    df_collision = ingest(spark, path)
    df_collision.repartition(8)
    logger.info(
        "[METRIC] [%s] dataset partitions (after repartition): %s",
        dataset_name,
        df_collision.rdd.getNumPartitions(),
    )

    ### COLLISION TRANSFORM
    df_collision_clean = transform_clean(df_collision, dataset_name)
    df_collision_curated = transform_curate(df_collision_clean, dataset_name)

    # ### VEHICLE INGEST
    dataset_name = "vehicle"
    path = DATA_PATH / dataset_name
    df_vehicle = ingest(spark, path)
    df_vehicle.repartition(8)
    logger.info(
        "[METRIC] [%s] dataset partitions (after repartition): %s",
        dataset_name,
        df_vehicle.rdd.getNumPartitions(),
    )

    ### VEHICLE TRANSFORM
    df_vehicle_clean = transform_clean(df_vehicle, dataset_name)
    df_vehicle_curated = transform_curate(df_vehicle_clean, dataset_name)

    # ### CASUALTY INGEST
    dataset_name = "casualty"
    path = DATA_PATH / dataset_name
    df_casualty = ingest(spark, path)
    df_casualty.repartition(8)
    logger.info(
        "[METRIC] [%s] dataset partitions (after repartition): %s",
        dataset_name,
        df_casualty.rdd.getNumPartitions(),
    )

    ### CASUALTY TRANSFORM
    df_casualty_clean = transform_clean(df_casualty, dataset_name)
    df_casualty_curated = transform_curate(df_casualty_clean, dataset_name)

    ### JOIN
    df_final = join_datasets(
        df_collision,
        df_vehicle,
        df_casualty,
    )
    
    df_final.show()
    

    spark.stop()
    logger.info("Successfully finished ETL pipeline (SPARK)")

    # TODO: review existing METRIC. Refactor - log all important metrics


if __name__ == "__main__":
    main()
