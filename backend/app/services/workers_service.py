import pandas as pd
from fastapi import HTTPException, UploadFile
from typing import List
from sqlmodel import Session, select
from datetime import datetime
import time
import traceback

from app.services.utils.upload_service import handle_file_upload_generic
from app.utils.validators.validate_excel_workers import validate_excel_workers
from app.core.workers_concentrix.merge_worker_cx import generate_worker_cx_table
from app.core.workers_ubycall.merge_worker_ubycall import generate_worker_uby_table
from app.crud.worker import upsert_lookup_table, bulk_upsert_workers
from app.models.worker import Role, Status, Campaign, Team, WorkType, ContractType, Worker
from app.services.utils.files_name import FILES_WORKER_SERVICE


def safe_date(value):
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.date()
    try:
        return pd.to_datetime(value).date()
    except Exception:
        return None


async def process_and_persist_workers(
    files: List[UploadFile],
    session: Session
) -> int:
    try:
        """Procesa los archivos Excel de trabajadores y persiste la información en BD con métricas de tiempo."""
        start_total = time.perf_counter()

        # 🕒 Tiempo 1: Lectura y concatenación
        t1 = time.perf_counter()
        people_active, people_inactive, scheduling_ppp, api_id, master_glovo, scheduling_ubycall = await handle_file_upload_generic(
            files=files,
            validator=validate_excel_workers,
            keyword_to_slot=FILES_WORKER_SERVICE,
            required_slots=list(FILES_WORKER_SERVICE.values()),
            post_process=lambda people_active, people_inactive, scheduling_ppp, api_id, master_glovo, scheduling_ubycall: (
                people_active, people_inactive, scheduling_ppp, api_id, master_glovo, scheduling_ubycall)
        )

        df_concentrix = generate_worker_cx_table(people_active, people_inactive,scheduling_ppp, api_id)


        df_ubycall = generate_worker_uby_table(master_glovo, scheduling_ubycall, api_id, people_active, people_inactive)


        df = pd.concat([df_concentrix, df_ubycall], ignore_index=True)

        df = df.where(pd.notnull(df), None)
        role_map = upsert_lookup_table(
            session, Role,       df["role"].tolist())
        status_map = upsert_lookup_table(
            session, Status,     df["status"].tolist())
        campaign_map = upsert_lookup_table(
            session, Campaign,   df["campaign"].tolist())
        team_map = upsert_lookup_table(
            session, Team,       df["team"].tolist())
        worktype_map = upsert_lookup_table(
            session, WorkType,   df["work_type"].tolist())
        contract_map = upsert_lookup_table(
            session, ContractType, df["contract_type"].tolist())

        # 🕒 Tiempo 3: Preparación de registros
        workers_data = []
        for row in df.to_dict(orient="records"):

            # 🔧 Normalización segura de requirement_id
            req_id = row.get("requirement_id")
            if pd.isna(req_id) or req_id is None:
                req_id = None
            else:
                req_id = str(req_id).strip()  # fuerza todo a string limpio

            workers_data.append({
                "document": str(row["document"]),
                "name": row["name"],
                "role_id": role_map.get(row["role"]),
                "status_id": status_map.get(row["status"]),
                "campaign_id": campaign_map.get(row["campaign"]),
                "team_id": team_map.get(row["team"]),
                "manager": row.get("manager"),
                "supervisor": row.get("supervisor"),
                "coordinator": row.get("coordinator"),
                "work_type_id": worktype_map.get(row["work_type"]),
                "start_date": safe_date(row.get("start_date")),
                "termination_date": safe_date(row.get("termination_date")),
                "contract_type_id": contract_map.get(row["contract_type"]),
                "requirement_id": req_id,  # 👈 ahora siempre str o None
                "api_id": row.get("api_id"),
                "api_name": row.get("api_name"),
                "api_email": row.get("api_email"),
                "observation_1": row.get("observation_1"),
                "observation_2": row.get("observation_2"),
                "tenure": row.get("tenure"),
                "trainee": row.get("trainee"),
                "productive": row.get("productive"),
            })

        # 🕒 Tiempo 4: Inserción masiva
        total_processed = bulk_upsert_workers(session, workers_data)

        return total_processed

    except Exception as e:
        print("❌ Error inesperado en process_and_persist_schedules:")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=500, detail=f"Internal error: {str(e)}")
