"""ETL pipeline - local file batch ingestion"""

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine

from pathlib import Path

RAW_DATA_FILE = "dft-road-casualty-statistics-collision-2023.csv"
CLEAN_DATA_FILE = "dft-road-casualty-statistics-collision-2023-clean.csv"
BASE_PATH = Path(__file__).resolve().parents[1]
RAW_DATA_FILE_PATH = BASE_PATH / "data" / "raw" / RAW_DATA_FILE
CLEAN_DATA_FILE_PATH = BASE_PATH / "data" / "processed" / CLEAN_DATA_FILE


def read_csv_file() -> DataFrame:
    """Returns a dataset from a CSV file"""
    print(f"Reading raw dataset CSV file: {RAW_DATA_FILE}")
    return pd.read_csv(RAW_DATA_FILE_PATH)


def load_csv_file(df: DataFrame) -> None:
    """Loads dataset into a file"""
    print(f"Loading dataset into {CLEAN_DATA_FILE}")
    try:
        df.to_csv(path_or_buf=CLEAN_DATA_FILE_PATH, index=False)
    except Exception as e:
        print(f"ERROR: Failed to load the dataset to CSV file, because of: {e}")
    print(f"Loaded clean dataset CSV file to: {CLEAN_DATA_FILE_PATH}")


def transform_data(df: DataFrame) -> DataFrame:
    """Returns a validated dataset"""

    print("Starting dataset transformation...")

    # Convert date type from string to datetime
    if {"date", "time"}.issubset(df.columns):
        dt_str = df["date"].astype(str) + "T" + df["time"].astype(str)
        df["collision_datetime"] = pd.to_datetime(
            arg=dt_str,
            format="%d/%m/%YT%H:%M",
            cache=True,
        )
        print(f"- Created new column collision_datetime")

    # Drop rows containing null values
    df = df.dropna()
    print("- Removed rows containing null values")

    # Drop duplicate rows
    df = df.drop_duplicates()
    print("- Removed duplicate rows")

    # Drop unnecessary columns
    drop_columns = ["date", "time"]
    df = df.drop(drop_columns, axis=1)
    print(f"- Removed unneccessairy columns: {drop_columns}")

    print("Finished dataset transfromation")
    return df


def load_data(df: DataFrame) -> None:
    """Loads the dataset to database"""
    print("Loading dataset to database...")

    # creates an in-memory sqlite database
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=False)

    rows_inserted = df.to_sql(
        name="data_projects",
        con=engine,
        # schema="project_p1",
        if_exists="replace",
        index=False,
    )

    # verify if any rows are inserted and if equals to number of rows to insert
    rows_to_insert = df.index.size
    if rows_inserted and rows_inserted == rows_to_insert:
        print(f"Loaded dataset to database.")
        print(
            f"From {rows_to_insert} rows to insert, {rows_inserted} rows were inserted."
        )

    else:
        print(
            f"ERROR: Something gone wrong. Could not load fully or partially the dataset to database."
        )
        print(f"\nRows to insert: {rows_to_insert}; Rows inserted: {rows_inserted}")


def etl() -> None:
    """ETL pipeline

    - Read a local CSV file
    - Transform the data if needed
    - Load the clean data info a CSV file
    - Load the data into a local SQLite DB
    """
    print("Running ETL pipeline....")

    # TODO - add error handling
    df = read_csv_file()
    df = transform_data(df)
    load_csv_file(df)
    load_data(df)

    print("Finished ETL pipeline.")


if __name__ == "__main__":
    etl()


# TODO
# CONTINUE HERE:
#     1. DATABASE AS A FILE - TO SEE HOW THE TYPES ARE INFERED
#     2. ENFORCE SCHEMA ON DATABASE INSERT
