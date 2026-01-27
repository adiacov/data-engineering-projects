# NOTE 1:
# This is an Airflow DAG
# If edited, make sure you copy / paste the file at:
# ./project_p4/.airflow/dags

# NOTE 2:
# To see the DAG in action, make sure you start Airflow services
# Run docker-compose.yaml from project-compose.yaml
# Goto: http://localhost:8080
# User: airflow
# Pass: airflow
# Find: main

from de_project import main_p1, main_p2, main_p3
from de_project.common.db import create_db_engine

from airflow.sdk import dag, task
from datetime import timedelta


@dag(
    dag_id="dag_collisions_pipeline",
    dag_display_name="dag_collisions_pipeline",
    catchup=False,
    max_consecutive_failed_dag_runs=2,
    default_args={
        # Tasks configuration
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
        "max_retry_delay": timedelta(hours=1),
        "retry_exponential_backoff": True,
    },
)
def main():
    """### DAG UK road traffic collisions ETL

    DAG executes the following tasks:
    - ingest raw data
    - transform raw to clean data
    - transform clean to curated data
    - model data for analytics
    """

    @task()
    def ingest_raw():
        """### ETL extract step.

        Reads a external CSV file and load it into database with minimal changes.
        """
        main_p1.main()

    @task()
    def transform_clean():
        """### ETL transform, load step.

        Reads an existing raw dataset from database,
        cleans the data by normalizing data types, column names.
        Do data validation for resulting dataset.
        Loads it as a clean dataset into DB.
        """
        engine = create_db_engine()
        main_p2.clean_dataset(engine)

    @task()
    def transform_curated():
        """### ETL transform, load step.

        Reads an existing clean dataset from database.
        Replaces codes to their meaningful names.
        Validates the resulting dataset.
        Loads it as a curated dataset into DB.
        """
        engine = create_db_engine()
        main_p2.curate_dataset(engine)

    @task()
    def model_analytics():
        """### Modeling step.

        Reads a curated dataset from database.
        Models the fact and dimension datasets.
        Loads resulting datasets into fact and dimension tables.
        """
        main_p3.main()

    (ingest_raw() >> transform_clean() >> transform_curated() >> model_analytics())


# DAG object for Airflow to load it.
dag = main()
