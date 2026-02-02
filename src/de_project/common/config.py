# env_vars.py
from dotenv import load_dotenv
from pathlib import Path
import os
import logging

logger = logging.getLogger(__name__)


def load_env():
    logger.info(f"Loading project environment variables")
    load_dotenv()


def is_runtime_local() -> bool:
    logger.info(f"Current APP runtime ENV: {os.environ.get("APP_RUNTIME")}")
    return os.environ.get("APP_RUNTIME") == "local"


def find_project_root(start: Path | None = None) -> Path:
    current = start or Path(__file__).resolve()

    for parent in [current, *current.parents]:
        if (parent / "pyproject.toml").exists():
            return parent

    raise RuntimeError("Project root not found (pyproject.toml missing)")


def get_data_path() -> Path:
    """Returns the path to the /data directory"""
    if is_runtime_local():
        return find_project_root() / "data"
    else:
        data_path = os.environ["PROJECT_DATA_DIR"]
        logger.info(f"Project data directory ENV: {data_path}")
        return Path(data_path)
