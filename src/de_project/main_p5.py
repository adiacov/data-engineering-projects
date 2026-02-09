from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as fc

import logging

from de_project.project_p5.spark_session import get_spark
from de_project.common.config import get_data_path
from de_project.common.logging_config import setup_logging
from de_project.project_p5.spark_ingest import ingest
from de_project.project_p5.transform.transform import transform_clean, transform_curate
from de_project.project_p5.spark_join import join_datasets
from de_project.project_p5.spark_write import write_to_parquet
from de_project.project_p5.spark_modelling import (
    build_collisions_fact,
    build_date_dimension,
    build_location_dimension,
    build_severity_dim,
    build_time_dim,
)


setup_logging()
logger = logging.getLogger(__name__)


def _run_collision_etl(spark, source_data_path) -> DataFrame:
    ### COLLISION INGEST
    dataset_name = "collision"
    path = source_data_path / dataset_name
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

    return df_collision_curated


def _run_vehicle_etl(spark, source_data_path) -> DataFrame:
    dataset_name = "vehicle"
    path = source_data_path / dataset_name
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

    return df_vehicle_curated


def _run_casualty_etl(spark, source_data_path) -> DataFrame:
    ### CASUALTY INGEST
    dataset_name = "casualty"
    path = source_data_path / dataset_name
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

    return df_casualty_curated


def main():
    logger.info("Starting ETL pipeline (SPARK)")

    DATA_PATH = get_data_path()
    DATA_PATH_RAW = DATA_PATH / "raw"
    DATA_PATH_OUTPUT = DATA_PATH / "output"

    spark: SparkSession = get_spark()

    # ### COLLISION DATASET
    # collision_df = _run_collision_etl(spark, DATA_PATH_RAW)
    # # facts and dimensions. Skip persist to database.
    # collision_fact = build_collisions_fact(collision_df)

    # ### VEHICLE DATASET
    # vehicle_df = _run_vehicle_etl(spark, DATA_PATH_RAW)

    # ### CASUALTY DATASET
    # casualty_df = _run_casualty_etl(spark, DATA_PATH_RAW)

    # ### JOIN
    # df_final = join_datasets(
    #     collision_fact,
    #     vehicle_df,
    #     casualty_df,
    # )

    # ### WRITE, PARTITION BY YEAR 2022 to 2023 inclusive
    # write_to_parquet(df_final, DATA_PATH_OUTPUT)

    ### READ PARTITIONED DATASET ONLY FOR A SPECIFIC YEAR
    df_2020 = spark.read.parquet(str(DATA_PATH_OUTPUT)).filter(
        fc.col("collision_year") == 2020
    )
    logger.info("Read collision parquet files for 2020 rows: %s", df_2020.count())

    logger.info("Spark plan for collision dataset: check filter")
    df_2020.explain()
    # PartitionFilters: [isnotnull(collision_year#54), (collision_year#54 = 2020)]

    spark.stop()
    logger.info("Successfully finished ETL pipeline (SPARK)")


if __name__ == "__main__":
    main()
