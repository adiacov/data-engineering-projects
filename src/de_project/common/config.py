# env_vars.py
from dotenv import load_dotenv
from pathlib import Path
import os
import logging

logger = logging.getLogger(__name__)


def load_env():
    load_dotenv(".project.env")
    logger.info(f"Current APP runtime: {os.environ.get("APP_RUNTIME")}")


def is_runtime_local() -> bool:
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
        return Path(os.environ["PROJECT_DATA_DIR"])
