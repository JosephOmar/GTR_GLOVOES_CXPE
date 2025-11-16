# app/handlers/planned_data.py

from fastapi import UploadFile
from app.services.utils.upload_service import handle_file_upload_generic
from app.utils.validators.validate_excel_planned import validate_excel_planned
from app.core.planned.clean_planned_data import clean_planned_data
from datetime import date, datetime
import pandas as pd
from fastapi import HTTPException
from sqlmodel import Session, select, delete
from app.models.planned import Planned

# Mapeo: keyword en filename validado → nombre del slot
_KPI_SLOTS = {
    "planned_data": "planned_data",
}

# Solo el planned_data es obligatorio para este caso
_REQUIRED_KPI = ["planned_data"]

# Helpers para parseo seguro


def safe_str(v) -> str | None:
    if v is None:
        return None
    return str(v).strip()


def safe_date(v) -> date | None:
    if v is None or pd.isna(v):
        return None
    if isinstance(v, datetime):
        return v.date()
    try:
        return pd.to_datetime(v).date()
    except:
        return None


def safe_int(v) -> int | None:
    try:
        return int(v) if v is not None else None
    except:
        return None


async def planned_service(file1: UploadFile, session: Session):
    print('llega aquí')
    try:
        df = await handle_file_upload_generic(
            files=[file1],
            validator=validate_excel_planned,
            keyword_to_slot=_KPI_SLOTS,
            required_slots=_REQUIRED_KPI,
            post_process=lambda planned_data, **kw: clean_planned_data(planned_data)
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    print('F')
    # 🔴 1) Eliminar todos los registros antes de insertar
    session.exec(delete(Planned))
    session.commit()

    rows_inserted = 0

    # 2) Insertar los nuevos registros
    for _, row in df.iterrows():
        planned = Planned(
            team=safe_str(row.get("team")),
            date=safe_date(row.get("date")),
            interval=safe_str(row.get("interval")),
            forecast_tht=safe_int(row.get("forecast_tht")),
            forecast_received=safe_int(row.get("forecast_received")),
            required_agents=safe_int(row.get("required_agents")),
            scheduled_agents=safe_int(row.get("scheduled_agents")),
        )
        session.add(planned)
        rows_inserted += 1

    # 3) Hacer commit de todos juntos (más eficiente que uno por fila)
    session.commit()

    return {"status": "success", "rows_inserted": rows_inserted}

