import pandas as pd
from fastapi import HTTPException
from typing import List
from sqlmodel import Session, select
from fastapi import UploadFile

from app.services.utils.upload_service import handle_file_upload_generic
from app.utils.validators.validate_excel_workers import validate_excel_workers
from app.core.workers_concentrix.merge_worker_cx import generate_worker_cx_table
from app.core.workers_ubycall.merge_worker_ubycall import generate_worker_uby_table
from app.crud.worker import upsert_lookup_table, upsert_worker
from app.models.worker import Role, Status, Campaign, Team, WorkType, ContractType, Worker
from datetime import datetime
import logging


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

async def process_and_persist_workers(
    files: List[UploadFile],  # UploadFile
    session: Session
) -> int:
    
    if not files:
        raise HTTPException(status_code=422, detail="No files uploaded")
    
    logging.info("Archivos recibidos:")
    for file in files:
        logging.info(f"Archivo: {file.filename}")

    # 1) Leer y limpiar data
    df = await handle_file_upload_generic(
        files=files,
        validator=validate_excel_workers,
        keyword_to_slot={
            "people_consultation":"people_consultation",
            "scheduling_ppp":"scheduling_ppp",
            "report_kustomer":"report_kustomer",
            "master_glovo":"master_glovo",
            "scheduling_ubycall":"scheduling_ubycall",
        },
        required_slots=["people_consultation","scheduling_ppp","report_kustomer"],
        post_process=lambda people_consultation, scheduling_ppp, report_kustomer, **slots: pd.concat([
            generate_worker_cx_table(people_consultation,scheduling_ppp,report_kustomer),
            generate_worker_uby_table(
                slots["master_glovo"],
                slots["scheduling_ubycall"],
                report_kustomer,
                people_consultation
            )
        ])
    )

    # 2) Mapeo lookups
    df = df.where(pd.notnull(df), None)
    role_map      = upsert_lookup_table(session, Role,       df["role"].tolist())
    status_map    = upsert_lookup_table(session, Status,     df["status"].tolist())
    campaign_map  = upsert_lookup_table(session, Campaign,   df["campaign"].tolist())
    team_map      = upsert_lookup_table(session, Team,       df["team"].tolist())
    worktype_map  = upsert_lookup_table(session, WorkType,   df["work_type"].tolist())
    contract_map  = upsert_lookup_table(session, ContractType,df["contract_type"].tolist())

    # 3) Crear instancias de Worker y persistir
    count_new = 0
    for row in df.to_dict(orient="records"):
        data = {
            "document":        str(row["document"]),
            "name":            row["name"],
            "role_id":         role_map.get(row["role"]),
            "status_id":       status_map.get(row["status"]),
            "campaign_id":     campaign_map.get(row["campaign"]),
            "team_id":         team_map.get(row["team"]),
            "manager":         row.get("manager"),
            "supervisor":      row.get("supervisor"),
            "coordinator":     row.get("coordinator"),
            "work_type_id":    worktype_map.get(row["work_type"]),
            "start_date":      safe_date(row.get("start_date")),
            "termination_date":safe_date(row.get("termination_date")),
            "contract_type_id":contract_map.get(row["contract_type"]),
            "requirement_id":  row.get("requirement_id"),
            "kustomer_id":     row.get("kustomer_id"),
            "kustomer_name":   row.get("kustomer_name"),
            "kustomer_email":  row.get("kustomer_email"),
            "observation_1":   row.get("observation_1"),
            "observation_2":   row.get("observation_2"),
            "tenure":          row.get("tenure"),
            "trainee":         row.get("trainee"),
        }
        # 1) Hacer el SELECT para ver si existe
        stmt = select(Worker).where(Worker.document == str(row["document"]))
        existing = session.exec(stmt).first()

        # 2) Si existe, limpiar horarios viejos
        if existing:
            existing.schedules.clear()
            existing.ubycall_schedules.clear()  

        # 3) Upsert de Worker (insert o update de campos)
        worker = upsert_worker(session, data)
        # Puedes opcionalmente contar sólo nuevos:
        if session.is_modified(worker, include_collections=False) and not getattr(worker, "id", None):
            count_new += 1

    session.commit()
    return len(df)
