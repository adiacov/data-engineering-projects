"""SQL engine management"""

from sqlalchemy import (
    create_engine,
    Engine,
    MetaData,
    Column,
    Table,
    Integer,
    String,
    Float,
    DateTime,
    Boolean,
)

from pathlib import Path
import logging

logger = logging.getLogger(__file__)


def _create_collisions_raw_table(metadata: MetaData) -> Table:
    """Returns a new table: collisions_raw"""
    table = Table(
        "collisions_raw",
        metadata,
        Column("collision_index", String),
        Column("collision_year", Integer),
        Column("collision_ref_no", String),
        Column("location_easting_osgr", Float, nullable=True),
        Column("location_northing_osgr", Float, nullable=True),
        Column("longitude", Float, nullable=True),
        Column("latitude", Float, nullable=True),
        Column("police_force", Integer),
        Column("collision_severity", Integer),
        Column("number_of_vehicles", Integer),
        Column("number_of_casualties", Integer),
        Column("day_of_week", Integer),
        Column("local_authority_district", Integer),
        Column("local_authority_ons_district", String),
        Column("local_authority_highway", String),
        Column("local_authority_highway_current", String),
        Column("first_road_class", Integer),
        Column("first_road_number", Integer),
        Column("road_type", Integer),
        Column("speed_limit", Integer),
        Column("junction_detail_historic", Integer),
        Column("junction_detail", Integer),
        Column("junction_control", Integer),
        Column("second_road_class", Integer),
        Column("second_road_number", Integer),
        Column("pedestrian_crossing_human_control_historic", Integer),
        Column("pedestrian_crossing_physical_facilities_historic", Integer),
        Column("pedestrian_crossing", Integer),
        Column("light_conditions", Integer),
        Column("weather_conditions", Integer),
        Column("road_surface_conditions", Integer),
        Column("special_conditions_at_site", Integer),
        Column("carriageway_hazards_historic", Integer),
        Column("carriageway_hazards", Integer),
        Column("urban_or_rural_area", Integer),
        Column("did_police_officer_attend_scene_of_accident", Integer),
        Column("trunk_road_flag", Integer),
        Column("lsoa_of_accident_location", String),
        Column("enhanced_severity_collision", Integer),
        Column("collision_injury_based", Integer),
        Column("collision_adjusted_severity_serious", Float),
        Column("collision_adjusted_severity_slight", Float),
        Column("collision_datetime", DateTime),
    )
    return table


def _create_collisions_clean_table(metadata: MetaData) -> Table:
    """Returns a new table: collisions_clean"""

    table = Table(
        "collisions_clean",
        metadata,
        Column("collision_id", String),
        Column("collision_year", Integer),
        Column("collision_ref_no", String),
        Column("location_easting_osgr", Float, nullable=True),
        Column("location_northing_osgr", Float, nullable=True),
        Column("longitude", Float, nullable=True),
        Column("latitude", Float, nullable=True),
        Column("police_force", String),
        Column("collision_severity", String),
        Column("number_of_vehicles", Integer),
        Column("number_of_casualties", Integer),
        Column("day_of_week", String),
        Column("local_authority_district", String),
        Column("local_authority_ons_district", String),
        Column("local_authority_highway", String),
        Column("local_authority_highway_current", String),
        Column("first_road_class", String),
        Column("first_road_number", String),
        Column("road_type", String),
        Column("speed_limit", String),
        Column("junction_detail_historic", String),
        Column("junction_detail", String),
        Column("junction_control", String),
        Column("second_road_class", String),
        Column("second_road_number", String),
        Column("pedestrian_crossing_human_control_historic", String),
        Column("pedestrian_crossing_physical_facilities_historic", String),
        Column("pedestrian_crossing", String),
        Column("light_conditions", String),
        Column("weather_conditions", String),
        Column("road_surface_conditions", String),
        Column("special_conditions_at_site", String),
        Column("carriageway_hazards_historic", String),
        Column("carriageway_hazards", String),
        Column("urban_or_rural_area", String),
        Column("did_police_officer_attend_scene_of_accident", String),
        Column("trunk_road_flag", String),
        Column("lsoa_of_accident_location", String),
        Column("enhanced_severity_collision", String),
        Column("collision_injury_based", String),
        Column("collision_adjusted_severity_serious", Float),
        Column("collision_adjusted_severity_slight", Float),
        Column("collision_datetime", DateTime),
    )
    return table


def _create_collisions_curated_table(metadata: MetaData) -> Table:
    """Returns a new table: collisions_curated"""

    table = Table(
        "collisions_curated",
        metadata,
        Column("collision_id", String),
        Column("collision_year", Integer),
        Column("collision_ref_no", String),
        Column("location_easting_osgr", Float, nullable=True),
        Column("location_northing_osgr", Float, nullable=True),
        Column("longitude", Float, nullable=True),
        Column("latitude", Float, nullable=True),
        Column("police_force", String),
        Column("collision_severity", String),
        Column("number_of_vehicles", Integer),
        Column("number_of_casualties", Integer),
        Column("day_of_week", String),
        Column("local_authority_district", String),
        Column("local_authority_ons_district", String),
        Column("local_authority_highway", String),
        Column("local_authority_highway_current", String),
        Column("first_road_class", String),
        Column("first_road_number", String),
        Column("road_type", String),
        Column("speed_limit", String),
        Column("junction_detail_historic", String),
        Column("junction_detail", String),
        Column("junction_control", String),
        Column("second_road_class", String),
        Column("second_road_number", String),
        Column("pedestrian_crossing_human_control_historic", String),
        Column("pedestrian_crossing_physical_facilities_historic", String),
        Column("pedestrian_crossing", String),
        Column("light_conditions", String),
        Column("weather_conditions", String),
        Column("road_surface_conditions", String),
        Column("special_conditions_at_site", String),
        Column("carriageway_hazards_historic", String),
        Column("carriageway_hazards", String),
        Column("urban_or_rural_area", String),
        Column("did_police_officer_attend_scene_of_accident", String),
        Column("trunk_road_flag", String),
        Column("lsoa_of_accident_location", String),
        Column("enhanced_severity_collision", String),
        Column("collision_injury_based", String),
        Column("collision_adjusted_severity_serious", Float),
        Column("collision_adjusted_severity_slight", Float),
        Column("collision_datetime", DateTime),
        Column("is_weekend_day", Boolean),
        Column("collision_time", String),
        Column("collision_year_month", String),
        Column("severity_group", String),
    )
    return table


def _create_ingestion_metadata_table(metadata: MetaData) -> Table:
    """Returns a new table: ingestion_metadata"""
    ingestion_metadata_table = Table(
        "ingestion_metadata",
        metadata,
        Column("dataset_name", String),
        Column("file_hash", Integer),
        Column("ingested_at", DateTime),
    )
    return ingestion_metadata_table


from de_project.common.config import is_runtime_local, get_data_path


def _create_db_url() -> str:
    # Declaring here for simplicity. Usually these are kept in config files

    if is_runtime_local():
        db_folder = Path.home() / ".cache" / "sqlite"
    else:
        db_folder = get_data_path() / "sqlite"

    db_folder.mkdir(parents=True, exist_ok=True)
    db_path = db_folder / "data_engineering.db"
    db_url = f"sqlite+pysqlite:///{db_path}"

    logger.info(f"Using SQLite database at: {db_url}")
    return db_url


def create_db_engine(echo: bool = False) -> Engine:
    """Creates a new database engine instance.

    :param echo=False: if True, the Engine will log all statements
    """
    db_url = _create_db_url()

    try:
        logging.info("Creating SQL engine")
        metadata = MetaData()
        _create_collisions_raw_table(metadata)
        _create_ingestion_metadata_table(metadata)
        _create_collisions_clean_table(metadata)
        _create_collisions_curated_table(metadata)
        engine = create_engine(db_url, echo=echo)
        metadata.create_all(engine)
    except Exception:
        logging.error("Failed to create SQL engine")
        raise

    logging.info("Successfully created SQL engine")
    return engine
