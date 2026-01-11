import pandera.pandas as pa
from datetime import datetime


class CollisionsRawSchema(pa.DataFrameModel):
    collision_index: str = pa.Field()
    collision_year: int = pa.Field()
    collision_ref_no: str = pa.Field()
    location_easting_osgr: float = pa.Field(nullable=True)
    location_northing_osgr: float = pa.Field(nullable=True)
    longitude: float = pa.Field(nullable=True)
    latitude: float = pa.Field(nullable=True)
    police_force: int = pa.Field()
    collision_severity: int = pa.Field()
    number_of_vehicles: int = pa.Field()
    number_of_casualties: int = pa.Field()
    day_of_week: int = pa.Field()
    local_authority_district: int = pa.Field()
    local_authority_ons_district: str = pa.Field()
    local_authority_highway: str = pa.Field()
    local_authority_highway_current: str = pa.Field()
    first_road_class: int = pa.Field()
    first_road_number: int = pa.Field()
    road_type: int = pa.Field()
    speed_limit: int = pa.Field()
    junction_detail_historic: int = pa.Field()
    junction_detail: int = pa.Field()
    junction_control: int = pa.Field()
    second_road_class: int = pa.Field()
    second_road_number: int = pa.Field()
    pedestrian_crossing_human_control_historic: int = pa.Field()
    pedestrian_crossing_physical_facilities_historic: int = pa.Field()
    pedestrian_crossing: int = pa.Field()
    light_conditions: int = pa.Field()
    weather_conditions: int = pa.Field()
    road_surface_conditions: int = pa.Field()
    special_conditions_at_site: int = pa.Field()
    carriageway_hazards_historic: int = pa.Field()
    carriageway_hazards: int = pa.Field()
    urban_or_rural_area: int = pa.Field()
    did_police_officer_attend_scene_of_accident: int = pa.Field()
    trunk_road_flag: int = pa.Field()
    lsoa_of_accident_location: str = pa.Field()
    enhanced_severity_collision: int = pa.Field()
    collision_injury_based: int = pa.Field()
    collision_adjusted_severity_serious: float = pa.Field()
    collision_adjusted_severity_slight: float = pa.Field()
    collision_datetime: datetime = pa.Field()

    class Config:
        coerce = True
        strict = True
        unique_column_names = True
