from pyspark.sql.types import StructType, StructField
from pyspark.sql import types as T


def _get_collision_schema():

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
            StructField("date", T.DateType(), True),
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


def get_schema_for(name: str) -> StructType | None:
    match name:
        case "collision":
            return _get_collision_schema()
        case _:
            return None
