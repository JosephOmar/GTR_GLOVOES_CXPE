# validators/workers.py

from app.utils.validators.common import validate_and_map_filename

# Mapeo específico
_WORKERS_MAPPING = {
    "ConsultaPersonal":         "people_consultation",
    "Programación Glovo":       "scheduling_ppp",
    "report":                   "report_kustomer",
    "Maestro_Glovo":            "master_glovo",
    "Ubycall":                  "scheduling_ubycall",
    "Reporte_Descarga_Horario": "schedule_concentrix"
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
