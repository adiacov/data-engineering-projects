import logging

from pyspark.sql import SparkSession
from pyspark import SparkConf

from de_project.common.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def get_spark():
    conf = (
        SparkConf()
        .setMaster(value="local[2]")
        .setAppName("ETL Pipeline (SPARK)")
        .set("spark.sql.shuffle.partitions", "8")
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark
