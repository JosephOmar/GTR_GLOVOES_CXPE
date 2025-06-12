# app/services/real_data_view_service.py
import pandas as pd
from datetime import datetime, date
from typing import List, Tuple

from fastapi import HTTPException
from sqlmodel import Session
from fastapi import UploadFile

from app.services.utils.upload_service import handle_file_upload_generic
from app.utils.validators.validate_excel_operational_view import validate_excel_operational_view
from app.core.operational_view.clean_real_data_view import merge_data_view
from app.crud.operational_view import (
    get_views_map_by_date,
    bulk_create_views
)
from app.models.operational_view import OperationalView

# Mismos slots y required que antes
_REAL_SLOTS = {
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
_REQUIRED_REAL = list(_REAL_SLOTS.values())

# Helpers para parseo seguro
def safe_str(v) -> str | None:
    if v is None: return None
    return str(v).strip()

def safe_date(v) -> date | None:
    if v is None or pd.isna(v): return None
    if isinstance(v, datetime): return v.date()
    try: return pd.to_datetime(v).date()
    except: return None

def safe_int(v) -> int | None:
    try: return int(v) if v is not None else None
    except: return None

def safe_float(v) -> float | None:
    try: return float(v) if v is not None else None
    except: return None

async def process_and_persist_operational_view(
    files: List[UploadFile],
    session: Session
) -> dict:
    # 1) Leer y mergear todos los Excels en un DataFrame limpio
    try:
        df = await handle_file_upload_generic(
            files=files,
            validator=validate_excel_operational_view,
            keyword_to_slot=_REAL_SLOTS,
            required_slots=_REQUIRED_REAL,
            post_process=lambda *args: merge_data_view(*args)
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 2) Normalizar nulos
    df = df.where(pd.notnull(df), None)

    # 3) Encontrar target_date (primer sat_interval no nulo)
    mask = df["sat_interval"].notna()
    if not mask.any():
        raise HTTPException(400, detail="No hay valores en 'sat_interval'")
    first_idx = df[mask].index[0]
    target_date = safe_date(df.at[first_idx, "date"])

    # 4) Filtrar sólo filas de target_date
    df["parsed_date"] = df["date"].apply(safe_date)
    df = df[df["parsed_date"] == target_date].drop(columns=["parsed_date"])

    # 5) Cargar existentes y preparar upserts
    existing_map = get_views_map_by_date(session, target_date)
    nuevos: list[OperationalView] = []

    # Campos que vamos a setear
    int_fields = [
        "forecast_received", "required_agents", "scheduled_agents",
        "forecast_hours", "scheduled_hours", "service_level",
        "real_received", "agents_online", "agents_training",
        "agents_aux", "sat_samples", "sat_ongoing", "sat_promoters",
        "sat_interval", "sat_abuser", "aht"
    ]

    for record in df.to_dict(orient="records"):
        key = (
            safe_str(record["team"]),
            target_date,
            safe_str(record["time_interval"])
        )
        # Update existing
        if key in existing_map:
            ev = existing_map[key]
            for f in int_fields:
                raw = record.get(f)
                val = safe_float(raw) if f in {
                    "forecast_hours","scheduled_hours","service_level",
                    "sat_ongoing","sat_promoters","sat_interval",
                    "sat_abuser","aht"
                } else safe_int(raw)
                setattr(ev, f, val)
        else:
            # Crear nuevo
            ov = OperationalView(
                team=key[0],
                date=key[1],
                time_interval=key[2]
            )
            for f in int_fields:
                raw = record.get(f)
                val = safe_float(raw) if f in {
                    "forecast_hours","scheduled_hours","service_level",
                    "sat_ongoing","sat_promoters","sat_interval",
                    "sat_abuser","aht"
                } else safe_int(raw)
                setattr(ov, f, val)
            nuevos.append(ov)

    # 6) Persistir nuevos y commitear
    if nuevos:
        bulk_create_views(session, nuevos)
    session.commit()

    total = len(df)
    return {
        "date": target_date.isoformat(),
        "total_rows": total,
        "inserted": len(nuevos),
        "updated": total - len(nuevos),
    }
