# app/handlers/planned_data.py

from fastapi import UploadFile
from app.services.utils.upload_service import handle_file_upload_generic
from app.utils.validators.validate_excel_report_kpis import validate_excel_report_kpis
from app.core.report_kpis.clean_planned_data import clean_planned_data

# Mapeo: keyword en filename validado → nombre del slot
_KPI_SLOTS = {
    "planned_data":        "planned_data",
    "scheduling_ppp":      "scheduling_ppp",
    "report_kustomer":     "report_kustomer",
    "master_glovo":        "master_glovo",
    "scheduling_ubycall":  "scheduling_ubycall",
}

# Solo el planned_data es obligatorio para este caso
_REQUIRED_KPI = ["planned_data"]

async def repot_kpis_service(file1: UploadFile):
    result = await handle_file_upload_generic(
        files=[file1],
        validator=validate_excel_report_kpis,
        keyword_to_slot=_KPI_SLOTS,
        required_slots=_REQUIRED_KPI,
        post_process=lambda planned_data, **kw: clean_planned_data(planned_data)
    )
    return result


