from pyspark.sql.types import StructType, StructField
from pyspark.sql import types as T


import logging

logger = logging.getLogger(__name__)


def _get_collision_schema():
    logger.info("Found schema for dataset [%s]", "collision")

    return StructType(
        [
            StructField("collision_index", T.StringType(), True),
            StructField("collision_year", T.IntegerType(), True),
            StructField("collision_ref_no", T.StringType(), True),
            StructField("location_easting_osgr", T.FloatType(), True),
            StructField("location_northing_osgr", T.FloatType(), True),
            StructField("longitude", T.FloatType(), True),
            StructField("latitude", T.FloatType(), True),
            StructField("police_force", T.IntegerType(), True),
            StructField("collision_severity", T.IntegerType(), True),
            StructField("number_of_vehicles", T.IntegerType(), True),
            StructField("number_of_casualties", T.IntegerType(), True),
            StructField("date", T.StringType(), True),
            StructField("day_of_week", T.IntegerType(), True),
            StructField("time", T.StringType(), True),
            StructField("local_authority_district", T.IntegerType(), True),
            StructField("local_authority_ons_district", T.StringType(), True),
            StructField("local_authority_highway", T.StringType(), True),
            StructField("local_authority_highway_current", T.StringType(), True),
            StructField("first_road_class", T.IntegerType(), True),
            StructField("first_road_number", T.IntegerType(), True),
            StructField("road_type", T.IntegerType(), True),
            StructField("speed_limit", T.IntegerType(), True),
            StructField("junction_detail_historic", T.IntegerType(), True),
            StructField("junction_detail", T.IntegerType(), True),
            StructField("junction_control", T.IntegerType(), True),
            StructField("second_road_class", T.IntegerType(), True),
            StructField("second_road_number", T.IntegerType(), True),
            StructField(
                "pedestrian_crossing_human_control_historic", T.IntegerType(), True
            ),
            StructField(
                "pedestrian_crossing_physical_facilities_historic",
                T.IntegerType(),
                True,
            ),
            StructField("pedestrian_crossing", T.IntegerType(), True),
            StructField("light_conditions", T.IntegerType(), True),
            StructField("weather_conditions", T.IntegerType(), True),
            StructField("road_surface_conditions", T.IntegerType(), True),
            StructField("special_conditions_at_site", T.IntegerType(), True),
            StructField("carriageway_hazards_historic", T.IntegerType(), True),
            StructField("carriageway_hazards", T.IntegerType(), True),
            StructField("urban_or_rural_area", T.IntegerType(), True),
            StructField(
                "did_police_officer_attend_scene_of_accident", T.IntegerType(), True
            ),
            StructField("trunk_road_flag", T.IntegerType(), True),
            StructField("lsoa_of_accident_location", T.StringType(), True),
            StructField("enhanced_severity_collision", T.IntegerType(), True),
            StructField("collision_injury_based", T.IntegerType(), True),
            StructField("collision_adjusted_severity_serious", T.FloatType(), True),
            StructField("collision_adjusted_severity_slight", T.FloatType(), True),
        ]
    )


def _get_vehicle_schema():
    logger.info("Found schema for dataset [%s]", "vehicle")

    return StructType(
        [
            StructField("collision_index", T.StringType(), True),
            StructField("collision_year", T.IntegerType(), True),
            StructField("collision_ref_no", T.StringType(), True),
            StructField("vehicle_reference", T.StringType(), True),
            StructField("vehicle_type", T.IntegerType(), True),
            StructField("towing_and_articulation", T.IntegerType(), True),
            StructField("vehicle_manoeuvre_historic", T.IntegerType(), True),
            StructField("vehicle_manoeuvre", T.IntegerType(), True),
            StructField("vehicle_direction_from", T.IntegerType(), True),
            StructField("vehicle_direction_to", T.IntegerType(), True),
            StructField(
                "vehicle_location_restricted_lane_historic", T.IntegerType(), True
            ),
            StructField("vehicle_location_restricted_lane", T.IntegerType(), True),
            StructField("junction_location", T.IntegerType(), True),
            StructField("skidding_and_overturning", T.IntegerType(), True),
            StructField("hit_object_in_carriageway", T.IntegerType(), True),
            StructField("vehicle_leaving_carriageway", T.IntegerType(), True),
            StructField("hit_object_off_carriageway", T.IntegerType(), True),
            StructField("first_point_of_impact", T.IntegerType(), True),
            StructField("vehicle_left_hand_drive", T.IntegerType(), True),
            StructField("journey_purpose_of_driver_historic", T.IntegerType(), True),
            StructField("journey_purpose_of_driver", T.IntegerType(), True),
            StructField("sex_of_driver", T.IntegerType(), True),
            StructField("age_of_driver", T.IntegerType(), True),
            StructField("age_band_of_driver", T.IntegerType(), True),
            StructField("engine_capacity_cc", T.IntegerType(), True),
            StructField("propulsion_code", T.IntegerType(), True),
            StructField("age_of_vehicle", T.IntegerType(), True),
            StructField("generic_make_model", T.StringType(), True),
            StructField("driver_imd_decile", T.IntegerType(), True),
            StructField("lsoa_of_driver", T.StringType(), True),
            StructField("escooter_flag", T.IntegerType(), True),
            StructField("driver_distance_banding", T.IntegerType(), True),
        ]
    )


def _get_casualty_schema():
    logger.info("Found schema for dataset [%s]", "casualty")

    return StructType(
        [
            StructField("collision_index", T.StringType(), True),
            StructField("collision_year", T.IntegerType(), True),
            StructField("collision_ref_no", T.StringType(), True),
            StructField("vehicle_reference", T.StringType(), True),
            StructField("casualty_reference", T.StringType(), True),
            StructField("casualty_class", T.IntegerType(), True),
            StructField("sex_of_casualty", T.IntegerType(), True),
            StructField("age_of_casualty", T.IntegerType(), True),
            StructField("age_band_of_casualty", T.StringType(), True),
            StructField("casualty_severity", T.IntegerType(), True),
            StructField("pedestrian_location", T.IntegerType(), True),
            StructField("pedestrian_movement", T.IntegerType(), True),
            StructField("car_passenger", T.IntegerType(), True),
            StructField("bus_or_coach_passenger", T.IntegerType(), True),
            StructField("pedestrian_road_maintenance_worker", T.IntegerType(), True),
            StructField("casualty_type", T.IntegerType(), True),
            StructField("casualty_imd_decile", T.IntegerType(), True),
            StructField("lsoa_of_casualty", T.StringType(), True),
            StructField("enhanced_casualty_severity", T.IntegerType(), True),
            StructField("casualty_injury_based", T.IntegerType(), True),
            StructField("casualty_adjusted_severity_serious", T.FloatType(), True),
            StructField("casualty_adjusted_severity_slight", T.FloatType(), True),
            StructField("casualty_distance_banding", T.IntegerType(), True),
        ]
    )



def get_schema_for(name: str) -> StructType | None:
    """Returns spark schema for a given dataset name"""

    logger.info("Searching schema for dataset [%s]", name)
    match name:
        case "collision":
            return _get_collision_schema()
        case "vehicle":
            return _get_vehicle_schema()
        case "casualty":
            return _get_casualty_schema()
        case _:
            return None
