"""Dataset column mappings from code to text values"""

import pandas as pd

from pathlib import Path

# Categorical mappings

_collision_severity_map = {
    1: "Fatal",
    2: "Serious",
    3: "Slight",
}

_enhanced_severity_collision_map = {
    1: "Fatal",
    5: "Very Serious",
    6: "Moderately Serious",
    7: "Less Serious",
    3: "Slight",
    -1: "Unknown",  # Data missing or out of range
}

_day_of_week_map = {
    1: "Sunday",
    2: "Monday",
    3: "Tuesday",
    4: "Wednesday",
    5: "Thursday",
    6: "Friday",
    7: "Saturday",
}


_first_road_class_map = {
    1: "Motorway",
    2: "A(M)",
    3: "A",
    4: "B",
    5: "C",
    6: "Unclassified",
    -1: "Unknown",  # "Data missing or out of range"
}

_road_type_map = {
    1: "Roundabout",
    2: "One way street",
    3: "Dual carriageway",
    6: "Single carriageway",
    7: "Slip road",
    12: "One way street/Slip road",
    9: "Unknown",  # this is not a fault
    -1: "Unknown",  # Data missing or out of range
}

_junction_detail_historic_map = {
    0: "Not at junction or within 20 metres",
    1: "Roundabout",
    2: "Mini-roundabout",
    3: "T or staggered junction",
    5: "Slip road",
    6: "Crossroads",
    7: "More than 4 arms (not roundabout)",
    8: "Private drive or entrance",
    9: "Other junction",
    99: "Unknown",  # unknown (self reported)
    -1: "Unknown",  # Data missing or out of range
}

_junction_detail_map = {
    0: "Not at junction or within 20 metres",
    13: "T or staggered junction",
    16: "Crossroads",
    17: "Junction with more than four arms (not roundabout)",
    18: "Using private drive or entrance",
    19: "Other junction",
    99: "Unknown",  # unknown (self reported)
    -1: "Unknown",  # Data missing or out of range
}

_junction_control_map = {
    0: "Not at junction or within 20 metres",
    1: "Authorised person",
    2: "Auto traffic signal",
    3: "Stop sign",
    4: "Give way or uncontrolled",
    -1: "Unknown",  # Data missing or out of range
    9: "Unknown",  # unknown (self reported)
}

_second_road_class_map = {
    0: "Not at junction or within 20 metres",
    1: "Motorway",
    2: "A(M)",
    3: "A",
    4: "B",
    5: "C",
    6: "Unclassified",
    9: "Unknown",  # Unknown (self rep only)
    -1: "Unknown",  # Data missing or out of range
}

_pedestrian_crossing_human_control_historic_map = {
    0: "None within 50 metres ",
    1: "Control by school crossing patrol",
    2: "Control by other authorised person",
    -1: "Unknown",  # Data missing or out of range
    9: "Unknown",  # unknown (self reported)
}

_pedestrian_crossing_physical_facilities_historic_map = {
    0: "No physical crossing facilities within 50 metres",
    1: "Zebra",
    4: "Pelican, puffin, toucan or similar non-junction pedestrian light crossing",
    5: "Pedestrian phase at traffic signal junction",
    7: "Footbridge or subway",
    8: "Central refuge",
    -1: "Unknown",  # Data missing or out of range
    9: "Unknown",  # unknown (self reported)
}

_pedestrian_crossing_map = {
    0: "No physical crossing facility within 50m",
    11: "Human crossing control by school crossing patrol",
    12: "Human crossing control by other authorised person",
    13: "Zebra crossing",
    14: "Pedestrian light crossing (pelican or puffin or toucan or similar)",
    15: "Pedestrian phase at traffic signal",
    16: "Footbridge or subway",
    17: "Central refuge - no other controls",
    99: "Unknown",  # unknown (self reported)
    -1: "Unknown",  # Data missing or out of range
}

_light_conditions_map = {
    1: "Daylight",
    4: "Darkness - lights lit",
    5: "Darkness - lights unlit",
    6: "Darkness - no lighting",
    7: "Darkness - lighting unknown",
    -1: "Unknown",  # Data missing or out of range
}

_weather_conditions_map = {
    1: "Fine no high winds",
    2: "Raining no high winds",
    3: "Snowing no high winds",
    4: "Fine + high winds",
    5: "Raining + high winds",
    6: "Snowing + high winds",
    7: "Fog or mist",
    8: "Other",
    9: "Unknown",  # unknown
    -1: "Unknown",  # Data missing or out of range
}

_road_surface_conditions_map = {
    1: "Dry",
    2: "Wet or damp",
    3: "Snow",
    4: "Frost or ice",
    5: "Flood over 3cm. deep",
    6: "Oil or diesel",
    7: "Mud",
    -1: "Unknown",  # Data missing or out of range
    9: "Unknown",  # unknown (self reported)
}

_special_conditions_at_site_map = {
    0: "None",
    1: "Auto traffic signal - out",
    2: "Auto signal part defective",
    3: "Road sign or marking defective or obscured",
    4: "Roadworks",
    5: "Road surface defective",
    6: "Oil or diesel",
    7: "Mud",
    -1: "Unknown",  # Data missing or out of range
    9: "Unknown",  # unknown (self reported)
}

_carriageway_hazards_historic_map = {
    0: "None",
    1: "Vehicle load on road",
    2: "Other object on road",
    3: "Previous accident",
    4: "Dog on road",
    5: "Other animal on road",
    6: "Pedestrian in carriageway - not injured",
    7: "Any animal in carriageway (except ridden horse)",
    -1: "Unknown",  # Data missing or out of range
    9: "Unknown",  # unknown (self reported)
}

_carriageway_hazards_map = {
    0: "None",
    11: "Defective traffic signals",
    12: "Permanent road signing or markings defective or obscured or inadequate",
    13: "Roadworks",
    14: "Oil or diesel",
    15: "Mud",
    16: "Dislodged vehicle load in carriageway",
    17: "Other object in carriageway",
    18: "Involvement with previous collision",
    19: "Pedestrian in carriageway - not injured",
    20: "Any animal in carriageway (except ridden horse)",
    21: "Poor or defective road surface",
    -1: "Unknown",  # Data missing or out of range
    99: "Unknown",  # unknown (self reported)
}

_urban_or_rural_area_map = {
    1: "Urban",
    2: "Rural",
    3: "Unallocated",
    -1: "Unknown",  # Data missing or out of range
}

_did_police_officer_attend_scene_of_accident_map = {
    1: "Yes",
    2: "No",
    3: "No - accident was reported using a self completion  form (self rep only)",
    -1: "Unknown",  # Data missing or out of range
}

_trunk_road_flag_map = {
    1: "Trunk (Roads managed by Highways England)",
    2: "Non-trunk",
    -1: "Unknown",  # Data missing or out of range
}

_collision_injury_based_map = {
    0: "Based on severity reporting",
    1: "Based on Injury code reporting",
}

# File based mappings

BASE_PATH_DATA_CODES = Path(__file__).resolve().parents[2] / "data" / "codes"
UK_LA_CODES_PATH = BASE_PATH_DATA_CODES / "uk-la-codes.csv"
UK_LSOA_CODES_PATH = BASE_PATH_DATA_CODES / "uk-lsoa-codes.csv"
UK_POLICE_FORCE_CODES_PATH = BASE_PATH_DATA_CODES / "uk-police-force-codes.csv"
UK_LAD_CODES_PATH = BASE_PATH_DATA_CODES / "uk-lad-codes.csv"


def _create_code_to_name_map(
    file_path: Path, key_col: str = "Code", value_col: str = "Name"
) -> dict:
    """Given a dataset file creates a dictionary with code as key and name as value.

    :param: key_col - the column serving as a key
    :parma: value_col - the column serving as a value

    :Example:
    CSV file:
    key_col,value_col,any_other_col,
    Code1,CodeName1,anything_else
    Code2,CodeName2,anything_else

    dict = _create_code_to_name_map(file_path, "key_col", "value_col")
    {"Code1": "CodeName1", "Code2": "CodeName2"}
    """
    df = pd.read_csv(file_path)
    df = df[[key_col, value_col]]

    return df.set_index(key_col, verify_integrity=True)[value_col].to_dict()


_police_force_map = _create_code_to_name_map(UK_POLICE_FORCE_CODES_PATH)
_local_authority_district_map = _create_code_to_name_map(UK_LAD_CODES_PATH)
_uk_la_map = _create_code_to_name_map(UK_LA_CODES_PATH)
_uk_lsoa_map = _create_code_to_name_map(UK_LSOA_CODES_PATH)


# Column mappings code to name
def get_mappings() -> dict[str, dict]:
    """Returns a specification for categorical mapping,
    where the key is the column name and
    the value is a dictionary of value-to-value transformation
    """
    return {
        "police_force": _police_force_map,
        "collision_severity": _collision_severity_map,
        "day_of_week": _day_of_week_map,
        "local_authority_district": _local_authority_district_map,
        "first_road_class": _first_road_class_map,
        "road_type": _road_type_map,
        "junction_detail_historic": _junction_detail_historic_map,
        "junction_detail": _junction_detail_map,
        "junction_control": _junction_control_map,
        "second_road_class": _second_road_class_map,
        "pedestrian_crossing_human_control_historic": _pedestrian_crossing_human_control_historic_map,
        "pedestrian_crossing_physical_facilities_historic": _pedestrian_crossing_physical_facilities_historic_map,
        "pedestrian_crossing": _pedestrian_crossing_map,
        "light_conditions": _light_conditions_map,
        "weather_conditions": _weather_conditions_map,
        "road_surface_conditions": _road_surface_conditions_map,
        "special_conditions_at_site": _special_conditions_at_site_map,
        "carriageway_hazards_historic": _carriageway_hazards_historic_map,
        "carriageway_hazards": _carriageway_hazards_map,
        "urban_or_rural_area": _urban_or_rural_area_map,
        "did_police_officer_attend_scene_of_accident": _did_police_officer_attend_scene_of_accident_map,
        "trunk_road_flag": _trunk_road_flag_map,
        "enhanced_severity_collision": _enhanced_severity_collision_map,
        "collision_injury_based": _collision_injury_based_map,
        "local_authority_ons_district": _uk_la_map,
        "local_authority_highway": _uk_la_map,
        "local_authority_highway_current": _uk_la_map,
        "lsoa_of_accident_location": _uk_lsoa_map,
    }
