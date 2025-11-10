# app/services/schedule_service.py

import pandas as pd
from fastapi import HTTPException, UploadFile
from typing import List
from datetime import date, timedelta
from sqlmodel import Session, select, delete, and_
import time  # ✅ para medir duración
import traceback

from app.services.utils.upload_service import handle_file_upload_generic
from app.utils.validators.validate_excel_schedule import validate_excel_schedule
from app.core.workers_schedule.schedule_concentrix import schedule_concentrix
from app.core.workers_schedule.schedule_ubycall import schedule_ubycall
from app.models.worker import Worker, Schedule, UbycallSchedule
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
    try:
        start_time = time.perf_counter()  # ⏱️ Inicio total

        # 1️⃣ Leer los Excels
        try:
            df_conc_raw, people_obs_raw, df_uby_raw = await handle_file_upload_generic(
                files=files,
                validator=validate_excel_schedule,
                keyword_to_slot={
                    "schedule_concentrix": "schedule_concentrix",
                    "people_obs": "people_obs",
                    "schedule_ubycall":   "schedule_ubycall"
                },
                required_slots=["schedule_concentrix", "people_obs", "schedule_ubycall"],
                post_process=lambda conc, people, uby: (conc, people, uby)
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        print('xd')
        print(week)
        print('xd')
        # 2️⃣ Calcular semana actual o especificada   
        today = date.today()
        year = year or today.year
        week = week or today.isocalendar()[1]
        monday_curr = date.fromisocalendar(year, week, 1)
        sunday_curr = monday_curr + timedelta(days=6)
        monday_prev = monday_curr - timedelta(days=7)

        # 3️⃣ Purga
        session.exec(delete(Schedule).where(Schedule.date < monday_prev))
        session.exec(delete(UbycallSchedule).where(UbycallSchedule.date < monday_prev))
        session.exec(delete(Schedule).where(and_(Schedule.date >= monday_curr, Schedule.date <= sunday_curr)))
        session.exec(delete(UbycallSchedule).where(and_(UbycallSchedule.date >= monday_curr, UbycallSchedule.date <= sunday_curr)))

        df_conc = schedule_concentrix(df_conc_raw, people_obs_raw, week=week, year=year)
        df_uby = schedule_ubycall(df_uby_raw)

        conc_docs = set(df_conc[DOCUMENT].astype(str).str.strip())
        uby_docs = set(df_uby[DOCUMENT].astype(str).str.strip())
        all_docs = conc_docs.union(uby_docs)

        existing_docs = set(
            session.exec(select(Worker.document).where(Worker.document.in_(all_docs))).all()
        )
        missing_docs = sorted(all_docs - existing_docs)

        conc_records = [
            {
                "worker_document": str(row.document).strip(),
                "date": row.date,
                "day": row.day,
                "start_time": row.start_time,
                "end_time": row.end_time,
                "break_start": getattr(row, "break_start", None),
                "break_end": getattr(row, "break_end", None),
                "is_rest_day": bool(getattr(row, "rest_day", False)),
                "obs": getattr(row, "obs", None),
            }
            for row in df_conc.itertuples(index=False)
            if str(row.document).strip() in existing_docs
        ]

        uby_records = [
            {
                "worker_document": str(row.document).strip(),
                "date": row.date,
                "day": row.day,
                "start_time": row.start_time,
                "end_time": row.end_time,
            }
            for row in df_uby.itertuples(index=False)
            if str(row.document).strip() in existing_docs
        ]

        if conc_records:
            session.bulk_insert_mappings(Schedule, conc_records)
        if uby_records:
            session.bulk_insert_mappings(UbycallSchedule, uby_records)
        session.commit()

        inserted_conc = len(conc_records)
        inserted_uby = len(uby_records)


        return {
            "inserted_concentrix": inserted_conc,
            "inserted_ubycall": inserted_uby,
            "missing_workers": missing_docs
        }
    except Exception as e: 
        print("❌ Error inesperado en process_and_persist_schedules:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")