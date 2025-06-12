# validators/workers.py

from app.utils.validators.common import validate_and_map_filename

# Mapeo específico
_SCHEDULE_MAPPING = {
    "schedule_concentrix": "schedule_concentrix",
    "schedule_ubycall": "schedule_ubycall",
}

def validate_excel_schedule(file_name: str) -> str:
    return validate_and_map_filename(
        file_name=file_name,
        name_mapping=_SCHEDULE_MAPPING,
        error_detail_no_keyword=(
            "El archivo debe tener un nombre válido "
        )
    )
