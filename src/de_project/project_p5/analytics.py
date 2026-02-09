from pyspark.sql import DataFrame, SparkSession

import logging

logger = logging.getLogger(__name__)

_monthly_collisions = """
    SELECT 
        year, 
        month, 
        COUNT(collision_count) as total_collisions 
    FROM collision_fact c
    JOIN date_dim d
        ON c.date_key = d.date_key
    GROUP BY year, month
    ORDER BY year, month
"""

_weekday_vs_weekend = """
    SELECT
        CASE
            WHEN is_weekend THEN "weekend"
            ELSE "weekday"
        END as day_type,
        COUNT(*) as total_collisions
    FROM collision_fact c
    JOIN date_dim d
        ON c.date_key = d.date_key
    GROUP BY day_type
"""


def run_analytics(
    spark: SparkSession,
    collision_fact: DataFrame,
    date_dim: DataFrame,
):
    """Displays analytics query results"""

    collision_fact.createOrReplaceTempView("collision_fact")
    date_dim.createOrReplaceTempView("date_dim")

    logger.info("Running query: Show monthly collisions count.")
    spark.sql(_monthly_collisions).show()

    logger.info("Running query: Weekday vs Weekend collisions count.")
    spark.sql(_weekday_vs_weekend).show()
