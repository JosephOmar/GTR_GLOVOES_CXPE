from fastapi import APIRouter, UploadFile, Depends
from sqlmodel import Session, select
from typing import Dict
from app.database.database import get_session  # Asegúrate que esto devuelve una Session de SQLModel
from app.services.workers_service import handle_file_upload_workers
from app.models.worker import Worker, Role, Status, Campaign, Team, WorkType, ContractType
import pandas as pd
from datetime import date, datetime

router = APIRouter()


# -------------------------
# Función para insertar valores únicos en una tabla relacionada
# -------------------------
def insert_unique_values(session: Session, model, column_values) -> Dict[str, int]:
    value_to_id = {}
    unique_values = column_values.dropna().unique()

    for value in unique_values:
        instance = session.exec(select(model).where(model.name == value)).first()
        if not instance:
            instance = model(name=value)
            session.add(instance)
            session.commit()
            session.refresh(instance)
        value_to_id[value] = instance.id

    return value_to_id

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
@router.post("/upload-workers/")
async def upload_workers(
    file1: UploadFile,
    file2: UploadFile,
    file3: UploadFile,
    file4: UploadFile,
    file5: UploadFile,
    session: Session = Depends(get_session)
):
    # Paso 1: Procesar los archivos y obtener el DataFrame limpio
    df = await handle_file_upload_workers(file1, file2, file3, file4, file5)
    df = df.where(pd.notnull(df), None) 

    # Paso 2: Poblar tablas relacionadas y obtener mapas de nombre → id
    role_map = insert_unique_values(session, Role, df["role"])
    status_map = insert_unique_values(session, Status, df["status"])
    campaign_map = insert_unique_values(session, Campaign, df["campaign"])
    team_map = insert_unique_values(session, Team, df["team"])
    work_type_map = insert_unique_values(session, WorkType, df["work_type"])
    contract_type_map = insert_unique_values(session, ContractType, df["contract_type"])

    # Paso 3: Insertar los workers
    for _, row in df.iterrows():
        worker = Worker(
            document=str(row["document"]),
            name=row["name"],
            role_id=role_map.get(row["role"]),
            status_id=status_map.get(row["status"]),
            campaign_id=campaign_map.get(row["campaign"]),
            team_id=team_map.get(row["team"]),
            manager=row.get("manager"),
            supervisor=row.get("supervisor"),
            coordinator=row.get("coordinator"),
            work_type_id=work_type_map.get(row["work_type"]),
            start_date=safe_date(row["start_date"]),
            termination_date=safe_date(row["termination_date"]),
            contract_type_id=contract_type_map.get(row["contract_type"]),
            requirement_id=safe_int(row.get("requirement_id")),
            kustomer_id=safe_str(row.get("kustomer_id")),
            kustomer_name=safe_str(row.get("kustomer_name")),
            kustomer_email=safe_str(row.get("kustomer_email")),
            observation_1=safe_str(row.get("observation_1")),
            observation_2=safe_str(row.get("observation_2")),
            tenure=safe_int(row.get("tenure")),
            trainee=safe_str(row.get("trainee"))
        )
        session.add(worker)

    session.commit()

    return {"message": f"Se insertaron {len(df)} trabajadores correctamente."}