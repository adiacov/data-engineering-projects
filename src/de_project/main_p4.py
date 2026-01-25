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


@dag(
    dag_id="dag_collisions_pipeline",
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
    def create_db_engine():
        """### Creates database engine.

        Returns:
            SqlAlchemy Engine
        """
        return create_db_engine()

    @task()
    def ingest_raw():
        """### ETL extract step.

        Reads a external CSV file and load it into database with minimal changes.
        """
        main_p1.main()

    @task()
    def transform_clean(engine):
        """### ETL transform, load step.

        Reads an existing raw dataset from database,
        cleans the data by normalizing data types, column names.
        Do data validation for resulting dataset.
        Loads it as a clean dataset into DB.
        """
        main_p2.clean_dataset(engine)

    @task()
    def transform_curated(engine):
        """### ETL transform, load step.

        Reads an existing clean dataset from database.
        Replaces codes to their meaningful names.
        Validates the resulting dataset.
        Loads it as a curated dataset into DB.
        """
        main_p2.curate_dataset(engine)

    @task()
    def model_analytics():
        """### Modeling step.

        Reads a curated dataset from database.
        Models the fact and dimension datasets.
        Loads resulting datasets into fact and dimension tables.
        """
        main_p3.main()

    engine = create_db_engine()
    (
        ingest_raw()
        >> transform_clean(engine)
        >> transform_curated(engine)
        >> model_analytics()
    )


# DAG object for Airflow to load it.
dag = main()
