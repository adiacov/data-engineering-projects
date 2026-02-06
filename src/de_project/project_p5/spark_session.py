import logging

from pyspark.sql import SparkSession
from pyspark import SparkConf

logger = logging.getLogger(__name__)


def _log_metrics(spark: SparkSession):
    """Logs important metrics"""
    conf = spark.conf

    logger.info("[METRIC] Spark app name: %s", conf.get("spark.app.name"))
    logger.info("[METRIC] Spark master: %s", conf.get("spark.master"))
    logger.info("[METRIC] Parallelism: %s", spark.sparkContext.defaultParallelism)
    logger.info(
        "[METRIC] Shuffle partitions: %s",
        conf.get("spark.sql.shuffle.partitions"),
    )


def get_spark():
    """Creates a Spark session"""

    logger.info("Creating Spark session...")

    conf = (
        SparkConf()
        .setMaster(value="local[2]")
        .setAppName("ETL Pipeline (SPARK)")
        .set("spark.sql.shuffle.partitions", "8")
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    _log_metrics(spark)
    logger.info("Spark session created")
    return spark
