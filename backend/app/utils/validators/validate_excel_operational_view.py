# validators/real_data_view.py

from app.utils.validators.common import validate_and_map_filename

# Mapeo específico
_OPERATIONAL_VIEW_MAPPING = {
    "planned_data": "planned_data",
    "talkdesk": "talkdesk",
    "assembled_call": "assembled_call",
    "sat_customer_total": "sat_customer_total",
    "sat_customer": "sat_customer",
    "sat_rider_total": "sat_rider_total",
    "sat_rider": "sat_rider",   
    "real_agents": "real_agents",
    "looker_customer": "looker_customer",
    "looker_rider": "looker_rider",
}

def validate_excel_operational_view(file_name: str) -> str:
    return validate_and_map_filename(
        file_name=file_name,
        name_mapping=_OPERATIONAL_VIEW_MAPPING
    )
