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
    session: Session = Depends(get_session)
):
    def is_record_complete(record: RealDataView) -> bool:
        if record.team == "CALL VENDORS":
            return all([
                record.forecast_received is not None,
                record.forecast_aht is not None,
                record.forecast_absenteeism is not None,
                record.required_agents is not None,
                record.scheduled_agents is not None,
                record.forecast_hours is not None,
                record.scheduled_hours is not None,
                record.service_level is not None,
                record.real_received is not None,
                # ❌ omitimos: sat_samples, sat_ongoing, sat_promoters, sat_interval
            ])
        else:
            return all([
                record.forecast_received is not None,
                record.forecast_aht is not None,
                record.forecast_absenteeism is not None,
                record.required_agents is not None,
                record.scheduled_agents is not None,
                record.forecast_hours is not None,
                record.scheduled_hours is not None,
                record.service_level is not None,
                record.real_received is not None,
                record.sat_samples is not None,
                record.sat_ongoing is not None,
                record.sat_promoters is not None,
            ])

    df = await handle_file_upload_real_data_view(file1, file2, file3, file4, file5, file6, file7, file8)
    df = df.where(pd.notnull(df), None)

    existing_records = session.exec(select(RealDataView)).all()
    existing_map = {
        (record.team, record.date, record.time_interval): record
        for record in existing_records
    }

    nuevos_registros = []
    current_date = date.today()  # Obtener la fecha actual

    for _, row in df.iterrows():
        team = str(row["team"]).strip()
        time_interval = str(row["time_interval"]).strip()
        date_val = safe_date(row["date"])

        key = (team, date_val, time_interval)
        if key in existing_map:
            record = existing_map[key]
            
            # Si el registro es del día actual, ignorar is_record_complete y actualizar todos los campos si no están vacíos
            if record.date == current_date:
                # Actualizar cualquier dato, incluso si está vacío
                val = safe_int(row["forecast_received"])
                if val is not None:
                    record.forecast_received = val

                val = safe_int(row["forecast_aht"])
                if val is not None:
                    record.forecast_aht = val

                val = safe_int(row["forecast_absenteeism"])
                if val is not None:
                    record.forecast_absenteeism = val

                val = safe_int(row["required_agents"])
                if val is not None:
                    record.required_agents = val

                val = safe_int(row["scheduled_agents"])
                if val is not None:
                    record.scheduled_agents = val

                if row["forecast_hours"] is not None:
                    record.forecast_hours = row["forecast_hours"]

                if row["scheduled_hours"] is not None:
                    record.scheduled_hours = row["scheduled_hours"]

                if row["service_level"] is not None:
                    record.service_level = row["service_level"]

                val = safe_int(row["real_received"])
                if val is not None:
                    record.real_received = val

                val = safe_int(row["real_agents"])
                if val is not None:
                    record.real_agents = val

                val = safe_int(row["sat_samples"])
                if val is not None:
                    record.sat_samples = val

                if row["sat_ongoing"] is not None:
                    record.sat_ongoing = row["sat_ongoing"]

                if row["sat_promoters"] is not None:
                    record.sat_promoters = row["sat_promoters"]

                if row["sat_interval"] is not None:
                    record.sat_interval = row["sat_interval"]

            # Si el registro no es del día actual y no está completo, solo actualizar los campos incompletos
            elif not is_record_complete(record):
                # Solo actualizar si el nuevo dato NO es None
                val = safe_int(row["forecast_received"])
                if val is not None:
                    record.forecast_received = val

                val = safe_int(row["forecast_aht"])
                if val is not None:
                    record.forecast_aht = val

                val = safe_int(row["forecast_absenteeism"])
                if val is not None:
                    record.forecast_absenteeism = val

                val = safe_int(row["required_agents"])
                if val is not None:
                    record.required_agents = val

                val = safe_int(row["scheduled_agents"])
                if val is not None:
                    record.scheduled_agents = val

                if row["forecast_hours"] is not None:
                    record.forecast_hours = row["forecast_hours"]

                if row["scheduled_hours"] is not None:
                    record.scheduled_hours = row["scheduled_hours"]

                if row["service_level"] is not None:
                    record.service_level = row["service_level"]

                val = safe_int(row["real_received"])
                if val is not None:
                    record.real_received = val

                val = safe_int(row["real_agents"])
                if val is not None:
                    record.real_agents = val

                val = safe_int(row["sat_samples"])
                if val is not None:
                    record.sat_samples = val

                if row["sat_ongoing"] is not None:
                    record.sat_ongoing = row["sat_ongoing"]

                if row["sat_promoters"] is not None:
                    record.sat_promoters = row["sat_promoters"]

                if row["sat_interval"] is not None:
                    record.sat_interval = row["sat_interval"]

        else:
            new_record = RealDataView(
                team=team,
                time_interval=time_interval,
                date=date_val,
                forecast_received=safe_int(row["forecast_received"]),
                forecast_aht=safe_int(row["forecast_aht"]),
                forecast_absenteeism=safe_int(row["forecast_absenteeism"]),
                required_agents=safe_int(row["required_agents"]),
                scheduled_agents=safe_int(row["scheduled_agents"]),
                forecast_hours=row["forecast_hours"],
                scheduled_hours=row["scheduled_hours"],
                service_level=row["service_level"],
                real_received=safe_int(row["real_received"]),
                real_agents=safe_int(row["real_agents"]),
                sat_samples=safe_int(row["sat_samples"]),
                sat_ongoing=row["sat_ongoing"],
                sat_promoters=row["sat_promoters"],
                sat_interval=row["sat_interval"]
            )
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
