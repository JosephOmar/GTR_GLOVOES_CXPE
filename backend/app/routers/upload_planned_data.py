from fastapi import APIRouter, UploadFile, Depends
from sqlmodel import Session, select
from typing import Dict
from app.database.database import get_session  # Asegúrate que esto devuelve una Session de SQLModel
from app.services.data_kpis_service import handle_file_upload_planned_data
from app.models.data_kpi import PlannedData
import pandas as pd
from datetime import date, datetime, time

router = APIRouter()

def safe_int(value):
    try:
        return int(value) if value is not None else None
    except:
        return None

def safe_str(value):
    try:
        return str(value) if value is not None else None
    except:
        return None

def safe_date(value):
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()  # Convierte datetime a date
    try:
        # Si viene como string, intenta parsear a date
        return pd.to_datetime(value).date()
    except Exception:
        return None
# -------------------------
# Ruta para subir y registrar los trabajadores
# -------------------------
@router.post("/upload-planned-data/")
async def upload_workers(
    file1: UploadFile,
    session: Session = Depends(get_session)
):
    # Paso 1: Procesar los archivos y obtener el DataFrame limpio
    df = await handle_file_upload_planned_data(file1)
    df = df.where(pd.notnull(df), None) 

    # Paso 2: Insertar la Planned Data
    for _, row in df.iterrows():
        plannedData = PlannedData(
            team = str(row["team"]),
            week = int(row["week"]),
            time_interval = str(row["time_interval"]),
            date = safe_date(row["date"]),
            forecast_received = safe_int(row["forecast_received"]),
            forecast_aht = safe_int(row["forecast_aht"]),
            forecast_absenteeism = safe_int(row["forecast_absenteeism"]),
            required_agents = safe_int(row["required_agents"]),
            scheduled_concentrix = safe_int(row["scheduled_concentrix"]),
            scheduled_ubycalls = safe_int(row["scheduled_ubycalls"]),
            forecast_hours = row["forecast_hours"],
            scheduled_hours = row["scheduled_hours"]
        )
        session.add(plannedData)

    session.commit()

    return {"message": f"Se insertaron {len(df)} datos planificados correctamente"}