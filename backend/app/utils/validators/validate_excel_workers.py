# validators/workers.py

from app.utils.validators.common import validate_and_map_filename

# Mapeo específico
_WORKERS_MAPPING = {
    "people_active":      "people_active",
    "people_inactive":      "people_inactive",
    "scheduling_ppp":       "scheduling_ppp",
    "api_id":                   "api_id",
    "master_glovo":            "master_glovo",
    "scheduling_ubycall":                  "scheduling_ubycall",
}


def validate_excel_workers(file_name: str) -> str:
    return validate_and_map_filename(
        file_name=file_name,
        name_mapping=_WORKERS_MAPPING,
        error_detail_no_keyword=(
            "El archivo debe tener un nombre válido "
            "(ConsultaPersonal, Programación Glovo, report, Maestro_Glovo, etc.)."
        )
    )
