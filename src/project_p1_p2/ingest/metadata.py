"""Ingestion metadata management"""

from sqlalchemy import Engine, text

import logging
from pathlib import Path
import hashlib
from datetime import datetime


logger = logging.getLogger(__name__)


# Validate new versus existing metadata
def validate_metadata(new_metadata: dict, current_metadata: dict) -> tuple[dict, bool]:
    """Validates new metadata dictionary against current one.

    :param new_metadata: The new metadata dictionary to validate
    :param current_metadata: The existing metadata

    Returns a tuple for metadata object and if file contents changed.
    The first tuple element:
        The `new_metadata` if `current_metadata` is empty.
        An empty dictionary if `new_metadata` is empty or validation didn't pass.
    The second tuple element:
        Whether the file contents changed or not (file_hash).
        This may be used as a signal to run or not the pipeline after validation step.
        Usually a valid metadata with unchanged file_hash means that the ingested file is the same.
    """

    logger.info("Validating ingestion metadata...")
    result = (new_metadata, True)
    if new_metadata:
        if current_metadata:
            result = _do_validate_metadata(new_metadata, current_metadata)

    return result


def _do_validate_metadata(new_metadata, current_metadata) -> tuple[dict, bool]:
    if set(new_metadata.keys()).difference(current_metadata.keys()):
        logger.warning("Metadata invalid, because of inconsistent keys")
        return ({}, False)
    elif new_metadata["dataset_name"] != current_metadata["dataset_name"]:
        logger.warning("Metadata invalid, because of different dataset being compared")
        return ({}, False)
    elif datetime.fromisoformat(new_metadata["ingested_at"]) <= datetime.fromisoformat(
        current_metadata["ingested_at"]
    ):
        logger.warning("Metadata invalid, because of ingestion date inconsistency")
        return ({}, False)

    if new_metadata["file_hash"] == current_metadata["file_hash"]:
        logger.info("Metadata valid and unchanged.")
        return (new_metadata, False)

    logger.info("Metadata valid.")
    return (new_metadata, True)


# Create metadata from a source file
def create_ingestion_metadata(file_path: Path) -> dict:
    """Create metadata info from a file"""

    file_name = file_path.name

    logger.info(f"Creating metadata for file {file_name}")
    with open(file_path, "rb") as file:
        digest = hashlib.file_digest(file, "sha256")
        try:
            result = {
                "dataset_name": file_name,
                "file_hash": digest.hexdigest(),
                "ingested_at": str(datetime.now()),
            }
            logger.info(f"Successfully created metadata from file")
            return result
        except FileNotFoundError:
            logger.error("File not found")
            raise
        except Exception:
            logger.error("Could not create metadata from file")
            raise


# Get existing ingestion metadata from database
def get_ingested_metadata(engine: Engine, dataset_path: Path) -> dict:
    """Returns ingestion dataset info for a given dataset"""

    dataset_name = dataset_path.name
    get_metadata_stmt = text(
        (
            "SELECT * FROM ingestion_metadata "
            "WHERE dataset_name = :dataset_name "
            "ORDER BY ingested_at DESC "
            "LIMIT 1"
        )
    )

    result = {}
    with engine.connect() as conn:
        try:
            sql_result = conn.execute(
                get_metadata_stmt,
                {"dataset_name": dataset_name},
            ).first()

            if sql_result:
                result = {
                    "dataset_name": sql_result.dataset_name,
                    "file_hash": sql_result.file_hash,
                    "ingested_at": str(sql_result.ingested_at),
                }
            return result

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
            "INSERT INTO ingestion_metadata(dataset_name, file_hash, ingested_at) "
            f"VALUES(:dataset_name, :file_hash, :ingested_at)"
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
