"""Ingestion metadata management"""

from sqlalchemy import Engine, text

import logging
from pathlib import Path
import os
import hashlib
from datetime import datetime


logger = logging.getLogger(__name__)


# Create metadata from a source file
def create_ingestion_medatada(file_path: Path) -> dict:
    """Create metadata info from a file"""

    file_name = file_path.name

    logger.info(f"Creating metadata for file {file_name}")
    with open(file_path, "rb") as file:
        digest = hashlib.file_digest(file, "sha256")
        try:
            result = {
                "dataset_name": file_name,
                "file_hash": digest.hexdigest(),
                "file_size_bytes": os.path.getsize(file_path),
                # "ingested_at": str(datetime.now()),
                "ingested_at": datetime.now(),
            }
            logger.info(f"Successfully created metadata from file")
            return result
        except FileNotFoundError:
            logger.error("File not found")
            raise
        except Exception:
            logger.error("Could not create metadata from file")
            raise


# Create metadata table
def create_metadata_table(engine: Engine) -> None:
    """Creates a database table for metadata information"""

    create_stmp = text(
        """
        CREATE TABLE IF NOT EXISTS ingestion_metadata(
        dataset_name TEXT, 
        file_hash TEXT, 
        file_size_bytes INTEGER, 
        ingested_at DATETIME
        )
        """
    )

    with engine.connect() as conn:
        try:
            logger.info("Create table if not exists")
            conn.execute(create_stmp)
            conn.commit()
        except Exception:
            logger.error("Could not create table ingestion_metadata")


# Get existing ingestion metadata from database
def get_ingested_metadata(engine: Engine, dataset_path: Path) -> dict:
    """Returns ingestion dataset info for a given dataset"""

    dataset_name = dataset_path.name
    get_metadata_stms = text(
        (
            "SELECT * FROM ingestion_metadata "
            "WHERE dataset_name = :dataset_name "
            "ORDER BY ingested_at DESC "
            "LIMIT 1"
        )
    )

    with engine.connect() as conn:
        try:
            result = conn.execute(
                get_metadata_stms,
                {"dataset_name": dataset_name},
            ).first()

            if result:
                return {
                    "dataset_name": result.dataset_name,
                    "file_hash": result.file_hash,
                    "file_size_bytes": result.file_size_bytes,
                    "ingested_at": str(result.ingested_at),
                }
            else:
                {}

        except Exception:
            logger.error(
                f"Could not find ingestion metadata for {dataset_name} dataset"
            )
            raise


# Load ingestion metadata into database
def load_metadata(engine: Engine, metadata: dict) -> None:
    """Inserts ingestion metadata into database"""

    insert_stmt = text(
        (
            "INSERT INTO ingestion_metadata(dataset_name, file_hash, file_size_bytes, ingested_at) "
            f"VALUES(:dataset_name, :file_hash, :file_size_bytes, :ingested_at)"
        )
    )

    logger.info("Loading ingestion metadata into database")
    with engine.connect() as conn:
        try:
            conn.execute(
                insert_stmt,
                metadata,
            )
            conn.commit()
            logger.info("Loaded ingestion metadata into database")
        except Exception:
            logger.error("Failed to load ingestion metadata into database")
            raise
