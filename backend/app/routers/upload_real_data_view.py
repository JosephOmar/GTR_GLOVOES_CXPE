from fastapi import APIRouter, UploadFile, Depends
from sqlmodel import Session, select
from typing import Dict, List
# Asegúrate que esto devuelve una Session de SQLModel
from app.database.database import get_session
from app.services.real_data_view_service import handle_file_upload_real_data_view
from app.models.real_data_view import RealDataView
import pandas as pd
from datetime import date, datetime, time

router = APIRouter()


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


def safe_int(value):
    """Convierte un valor a int, devolviendo None si no es posible."""
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def safe_float(value):
    """Convierte un valor a float, devolviendo None si no es posible."""
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None


@router.post("/upload-real-data-view/")
async def upload_real_data(
    file1: UploadFile,
    file2: UploadFile,
    file3: UploadFile,
    file4: UploadFile,
    file5: UploadFile,
    file6: UploadFile,
    file7: UploadFile,
    file8: UploadFile,
    file9: UploadFile,
    file10: UploadFile,
    session: Session = Depends(get_session)
):
    def is_record_complete(record: RealDataView) -> bool:
        required_fields = [
            "forecast_received", "required_agents", "scheduled_agents", "forecast_hours",
            "scheduled_hours", "service_level", "real_received", "agents_online", "agents_training", "agents_aux", "aht"
        ]
        if record.team != "CALL VENDORS":
            required_fields.extend([
                "sat_samples", "sat_ongoing", "sat_promoters", "sat_interval", "sat_abuser"
            ])
        return all(getattr(record, field) is not None for field in required_fields)

    # Simplify the file upload process
    df = await handle_file_upload_real_data_view(file1, file2, file3, file4, file5, file6, file7, file8, file9, file10)
    df = df.where(pd.notnull(df), None)

    existing_records = session.exec(select(RealDataView)).all()
    existing_map = {
        (record.team, record.date, record.time_interval): record
        for record in existing_records
    }

    nuevos_registros = []
    current_date = date.today()

    # Helper function to update or create records
    def update_or_create_record(row, record=None):
        fields_to_update = [
            "forecast_received","required_agents", "scheduled_agents", "forecast_hours",
            "scheduled_hours", "service_level", "real_received",
            "agents_online", "agents_training", "agents_aux", "sat_samples", "sat_ongoing", "sat_promoters", "sat_interval",
            "sat_abuser", "aht"
        ]

        
        for field in fields_to_update:

            raw = row.get(field)

            # Si la fila trae None, asignamos None explícitamente para que quede NULL en BD
            if raw is None:
                setattr(record, field, None)
                continue
            
            if field in ["forecast_hours", "scheduled_hours","service_level", "sat_ongoing", "sat_promoters", "sat_interval","sat_abuser", "aht"]:
                # Use safe_float for fields that are expected to be floats
                val = safe_float(row.get(field))
            else:
                # Use safe_int for the rest
                val = safe_int(row.get(field))

            if val is not None:
                setattr(record, field, val)
        return record

    for _, row in df.iterrows():
        team = str(row["team"]).strip()
        time_interval = str(row["time_interval"]).strip()
        date_val = safe_date(row["date"])

        key = (team, date_val, time_interval)
        if key in existing_map:
            record = existing_map[key]

            if record.date == current_date:
                # Update all fields, even if empty
                record = update_or_create_record(row, record)

            elif not is_record_complete(record):
                # Update only missing fields
                record = update_or_create_record(row, record)

        else:
            new_record = RealDataView(
                team=team,
                time_interval=time_interval,
                date=date_val
            )
            new_record = update_or_create_record(row, new_record)
            nuevos_registros.append(new_record)

    if nuevos_registros:
        session.bulk_save_objects(nuevos_registros)

    session.commit()

    return {
        "message": f"Datos procesados: {len(df)} filas. Nuevos: {len(nuevos_registros)}. Actualizados: {len(df) - len(nuevos_registros)}."
    }


@router.get("/real-data-view/", response_model=List[RealDataView])
def get_real_data_view(session: Session = Depends(get_session)):
    results = session.exec(select(RealDataView)).all()
    return results
