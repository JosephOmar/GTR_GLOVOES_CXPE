# validators/kpis.py

from app.utils.validators.common import validate_and_map_filename

# Mapeo específico
_ATTENDANCE_MAPPING = {
    "attendance": "attendance",
}

def validate_excel_attendance(file_name: str) -> str:
    return validate_and_map_filename(
        file_name=file_name,
        name_mapping=_ATTENDANCE_MAPPING,
        error_detail_no_keyword="El archivo debe tener un nombre válido (attendance)."
    )
