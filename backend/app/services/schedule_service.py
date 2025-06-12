# app/services/schedule_service.py

import pandas as pd
from fastapi import HTTPException, UploadFile
from typing import List
from sqlmodel import Session, select

from app.services.utils.upload_service import handle_file_upload_generic
from app.utils.validators.validate_excel_schedule import validate_excel_schedule
from app.core.workers_schedule.schedule_concentrix import schedule_concentrix
from app.core.workers_schedule.schedule_ubycall import schedule_ubycall
from app.models.worker import Worker
from app.models.worker import Schedule, UbycallSchedule
from app.core.utils.workers_cx.columns_names import DOCUMENT, DATE, START_TIME, END_TIME, DAY, BREAK_START, BREAK_END, REST_DAY

async def process_and_persist_schedules(
    files: List[UploadFile],
    session: Session
) -> dict:
    """
    1) Lee dos Excels ("concentrix" y "ubycall") con handle_file_upload_generic
    2) Limpia cada DataFrame con schedule_concentrix y schedule_ubycall
    3) Valida qué DOCUMENTS existen en Worker
    4) Inserta sólo los que existen y coleta los faltantes
    Devuelve conteos e listado de documentos no encontrados.
    """
    # 1) Carga “cruda”
    try:
        df_conc_raw, df_uby_raw = await handle_file_upload_generic(
            files=files,
            validator=validate_excel_schedule,
            keyword_to_slot={"schedule_concentrix": "schedule_concentrix", "schedule_ubycall": "schedule_ubycall"},
            required_slots=["schedule_concentrix", "schedule_ubycall"],
            post_process=lambda conc, uby: (conc, uby)
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 2) Limpieza con tus funciones core
    df_conc = schedule_concentrix(df_conc_raw)
    df_uby  = schedule_ubycall(df_uby_raw)

    # 3) Validación de documentos
    #   extraemos únicos y limpiamos espacios
    conc_docs = set(df_conc[DOCUMENT].astype(str).str.strip())
    uby_docs  = set(df_uby[DOCUMENT].astype(str).str.strip())
    all_docs  = conc_docs.union(uby_docs)

    existing = session.exec(
        select(Worker.document).where(Worker.document.in_(all_docs))
    ).all()
    existing_set = set(existing)
    missing_docs = sorted(all_docs - existing_set)

    # 4) Inserción selectiva
    inserted_conc = 0
    for row in df_conc.itertuples(index=False):
        doc = str(row.document).strip()
        if doc not in existing_set:
            continue
        sched = Schedule(
            worker_document=doc,
            date=row.date,
            day=row.day,
            start_time=row.start_time,
            end_time=row.end_time,
            break_start=row.break_start,
            break_end=row.break_end,
            is_rest_day=bool(row.rest_day),
        )
        session.add(sched)
        inserted_conc += 1

    inserted_uby = 0
    for row in df_uby.itertuples(index=False):
        doc = str(row.document).strip()
        if doc not in existing_set:
            continue
        uby = UbycallSchedule(
            worker_document=doc,
            date=row.date,
            day=row.day,
            start_time=row.start_time,
            end_time=row.end_time,
        )
        session.add(uby)
        inserted_uby += 1

    session.commit()

    return {
        "inserted_concentrix": inserted_conc,
        "inserted_ubycall": inserted_uby,
        "missing_workers": missing_docs
    }
