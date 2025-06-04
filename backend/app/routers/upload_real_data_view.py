from fastapi import APIRouter, UploadFile, Depends
from sqlmodel import Session, select
from typing import List
from app.database.database import get_session
from app.services.real_data_view_service import handle_file_upload_real_data_view
from app.models.real_data_view import RealDataView
import pandas as pd
from datetime import date, datetime

router = APIRouter()


def safe_str(value):
    try:
        return str(value) if value is not None else None
    except:
        return None


def safe_date(value):
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


def safe_int(value):
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def safe_float(value):
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
    # 1) Leer y limpiar la DataFrame
    df = await handle_file_upload_real_data_view(
        file1, file2, file3, file4, file5,
        file6, file7, file8, file9, file10
    )
    df = df.where(pd.notnull(df), None)

    # 2) Detectar target_date (primer valor no nulo en sat_interval)
    target_date = None
    mask_sat = df["sat_interval"].notna()
    if mask_sat.any():
        first_idx = df[mask_sat].index[0]
        raw_date = df.at[first_idx, "date"]
        target_date = safe_date(raw_date)

    if target_date is None:
        return {"error": "No se encontró ningún valor no nulo en la columna 'sat_interval'."}

    # 3) Filtrar df para quedarnos solo con filas de target_date
    df["safe_date"] = df["date"].apply(safe_date)
    df = df[df["safe_date"] == target_date].drop(columns=["safe_date"])

    # 4) Traer registros existentes solo de target_date
    existing_records = session.exec(
        select(RealDataView).where(RealDataView.date == target_date)
    ).all()
    existing_map = {
        (record.team, record.date, record.time_interval): record
        for record in existing_records
    }

    nuevos_registros = []

    # Función helper para asignar campos
    def update_or_create_record(row, record=None):
        fields_to_update = [
            "forecast_received", "required_agents", "scheduled_agents",
            "forecast_hours", "scheduled_hours", "service_level",
            "real_received", "agents_online", "agents_training",
            "agents_aux", "sat_samples", "sat_ongoing", "sat_promoters",
            "sat_interval", "sat_abuser", "aht"
        ]

        for field in fields_to_update:
            raw = row.get(field)

            if raw is None:
                setattr(record, field, None)
                continue

            if field in [
                "forecast_hours", "scheduled_hours", "service_level",
                "sat_ongoing", "sat_promoters", "sat_interval",
                "sat_abuser", "aht"
            ]:
                val = safe_float(raw)
            else:
                val = safe_int(raw)

            if val is not None:
                setattr(record, field, val)
        return record

    # 5) Iterar solo las filas de df (de target_date)
    for _, row in df.iterrows():
        team = safe_str(row["team"]).strip()
        time_interval = safe_str(row["time_interval"]).strip()
        date_val = safe_date(row["date"])  # coincide con target_date

        key = (team, date_val, time_interval)
        if key in existing_map:
            record = existing_map[key]
            # Siempre sobreescribimos todo el registro
            record = update_or_create_record(row, record)

        else:
            new_record = RealDataView(
                team=team,
                time_interval=time_interval,
                date=date_val
            )
            new_record = update_or_create_record(row, new_record)
            nuevos_registros.append(new_record)

    # 6) Guardar nuevos y commit
    if nuevos_registros:
        session.bulk_save_objects(nuevos_registros)

    session.commit()

    return {
        "message": (
            f"Datos procesados para la fecha {target_date.isoformat()}: "
            f"{len(df)} filas totales. Nuevos: {len(nuevos_registros)}. "
            f"Actualizados: {len(df) - len(nuevos_registros)}."
        )
    }


@router.get("/real-data-view/", response_model=List[RealDataView])
def get_real_data_view(session: Session = Depends(get_session)):
    results = session.exec(select(RealDataView)).all()
    return results
