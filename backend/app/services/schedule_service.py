# app/services/schedule_service.py

import pandas as pd
from fastapi import HTTPException, UploadFile
from typing import List
from datetime import date, timedelta
from sqlmodel import Session, select, delete, and_

from app.services.utils.upload_service import handle_file_upload_generic
from app.utils.validators.validate_excel_schedule import validate_excel_schedule
from app.core.workers_schedule.schedule_concentrix import schedule_concentrix
from app.core.workers_schedule.schedule_ubycall import schedule_ubycall
from app.models.worker import Worker
from app.models.worker import Schedule, UbycallSchedule
from app.core.utils.workers_cx.columns_names import (
    DOCUMENT, DATE, START_TIME, END_TIME,
    DAY, BREAK_START, BREAK_END, REST_DAY
)

async def process_and_persist_schedules(
    files: List[UploadFile],
    session: Session,
    week: int | None = None,
    year: int | None = None,
) -> dict:
    # 1) Leer los Excels
    try:
        df_conc_raw, df_uby_raw = await handle_file_upload_generic(
            files=files,
            validator=validate_excel_schedule,
            keyword_to_slot={
                "schedule_concentrix": "schedule_concentrix",
                "schedule_ubycall":   "schedule_ubycall"
            },
            required_slots=["schedule_concentrix", "schedule_ubycall"],
            post_process=lambda conc, uby: (conc, uby)
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 2) Calcular fechas de semana actual (o pasada por parámetro)
    today = date.today()
    year = year or today.year
    week = week or today.isocalendar()[1]

    # Lunes y domingo de la semana “activa”
    monday_curr = date.fromisocalendar(year, week, 1)
    sunday_curr = monday_curr + timedelta(days=6)
    # Lunes de la semana anterior
    monday_prev = monday_curr - timedelta(days=7)

    # 3) Purga:
    #  3.1) elimina TODO lo anterior a monday_prev
    session.exec(
        delete(Schedule)
        .where(Schedule.date < monday_prev)
    )
    session.exec(
        delete(UbycallSchedule)
        .where(UbycallSchedule.date < monday_prev)
    )
    #  3.2) elimina SOLO la semana “activa” (para reemplazarla)
    session.exec(
        delete(Schedule)
        .where(
            and_(
                Schedule.date >= monday_curr,
                Schedule.date <= sunday_curr
            )
        )
    )
    session.exec(
        delete(UbycallSchedule)
        .where(
            and_(
                UbycallSchedule.date >= monday_curr,
                UbycallSchedule.date <= sunday_curr
            )
        )
    )

    # 4) Generar nuevos registros SOLO para la semana activa
    df_conc = schedule_concentrix(df_conc_raw, week=week, year=year)
    df_uby  = schedule_ubycall(df_uby_raw)  # si quieres análogo, haz que acepte week/year

    # 5) Validar cuáles documentos existen
    conc_docs = set(df_conc[DOCUMENT].astype(str).str.strip())
    uby_docs  = set(df_uby[DOCUMENT].astype(str).str.strip())
    all_docs  = conc_docs.union(uby_docs)

    existing = session.exec(
        select(Worker.document).where(Worker.document.in_(all_docs))
    ).all()
    existing_set = set(existing)
    missing_docs = sorted(all_docs - existing_set)

    # 6) Insertar solo los válidos
    inserted_conc = 0
    for row in df_conc.itertuples(index=False):
        doc = str(row.document).strip()
        if doc not in existing_set:
            continue
        session.add(Schedule(
            worker_document=doc,
            date=row.date,
            day=row.day,
            start_time=row.start_time,
            end_time=row.end_time,
            break_start=row.break_start,
            break_end=row.break_end,
            is_rest_day=bool(row.rest_day),
        ))
        inserted_conc += 1

    inserted_uby = 0
    for row in df_uby.itertuples(index=False):
        doc = str(row.document).strip()
        if doc not in existing_set:
            continue
        session.add(UbycallSchedule(
            worker_document=doc,
            date=row.date,
            day=row.day,
            start_time=row.start_time,
            end_time=row.end_time,
        ))
        inserted_uby += 1

    session.commit()

    return {
        "inserted_concentrix": inserted_conc,
        "inserted_ubycall":   inserted_uby,
        "missing_workers":    missing_docs
    }
