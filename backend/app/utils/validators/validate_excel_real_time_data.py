# validators/kpis.py

from app.utils.validators.common import validate_and_map_filename

# Mapeo específico
_KPI_MAPPING = {
    "real_time_data": "real_time_data",
}

def validate_excel_real_time_data(file_name: str) -> str:
    return validate_and_map_filename(
        file_name=file_name,
        name_mapping=_KPI_MAPPING,
        error_detail_no_keyword="El archivo debe tener un nombre válido (real_time_data)."
    )
