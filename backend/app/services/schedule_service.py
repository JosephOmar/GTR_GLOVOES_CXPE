import pandas as pd
from fastapi import HTTPException, UploadFile
from typing import List
from datetime import date, timedelta
from sqlmodel import Session, select, delete, and_, func
import time
import datetime

from app.services.utils.upload_service import handle_file_upload_generic
from app.utils.validators.validate_excel_schedule import validate_excel_schedule
from app.core.workers_schedule.schedule_concentrix import schedule_concentrix
from app.core.workers_schedule.schedule_ubycall import schedule_ubycall
from app.core.workers_schedule.schedule_concentrix_ppp import schedule_ppp
from app.models.worker import Worker, Schedule, UbycallSchedule
from app.core.utils.workers_cx.columns_names import DOCUMENT, DATE, START_TIME, END_TIME, DAY, BREAK_START, BREAK_END, REST_DAY

def merge_schedule_concentrix(df_conc, df_ppp):
    df_conc["document"] = df_conc["document"].astype(str)
    print(df_conc[df_conc['document'] == '75350625'])
    df_ppp["document"] = df_ppp["document"].astype(str)
    print(df_ppp[df_ppp['document'] == '75350625'])

    cols_replace = ["start_time", "end_time", "break_start", "break_end", "rest_day"]

    df_merged = df_conc.merge(
        df_ppp[["document", "start_date", "end_date"] + cols_replace],
        on=["document", "start_date"],
        how="left",
        suffixes=("", "_ppp")
    )

    print(df_merged[df_merged['document'] == '75350625'])

    # Reemplazar solo cuando PPP tiene datos
    for col in cols_replace:
        df_merged[col] = df_merged[f"{col}_ppp"].combine_first(df_merged[col])

    # mantener end_date de conc si PPP no tiene
    df_merged["end_date"] = df_merged["end_date_ppp"].combine_first(df_merged["end_date"])

    print(df_merged[df_merged['document'] == '75350625'])

    # eliminar columnas auxiliares
    df_merged = df_merged.drop(columns=[f"{col}_ppp" for col in cols_replace] + ["end_date_ppp"])

    print(df_merged[df_merged['document'] == '75350625'])

    # reemplazar NaN por None
    df_merged = df_merged.where(pd.notnull(df_merged), None)

    return df_merged




# Función para dividir los registros en lotes de un tamaño determinado (por defecto, 2000 registros)
def chunked(iterable, size=2000):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]

async def process_and_persist_schedules(
    files: List[UploadFile],
    session: Session,
    week: int | None = None,
    year: int | None = None,
) -> dict:
    start_total = time.perf_counter()
    print("\n📂 [SCHEDULES] Iniciando process_and_persist_schedules (multi-horario Ubycall)")

    old_autoflush = session.autoflush
    old_expire_on_commit = session.expire_on_commit
    session.autoflush = False
    session.expire_on_commit = False

    try:
        # 1️⃣ Leer Excels
        t0 = time.perf_counter()
        try:
            df_conc_raw, people_obs_raw, df_uby_raw, df_ppp_raw = await handle_file_upload_generic(
                files=files,
                validator=validate_excel_schedule,
                keyword_to_slot={
                    "schedule_concentrix": "schedule_concentrix",
                    "people_obs": "people_obs",
                    "schedule_ubycall": "schedule_ubycall",
                    "schedule_ppp": "schedule_ppp"
                },
                required_slots=["schedule_concentrix", "people_obs", "schedule_ubycall", "schedule_ppp"],
                post_process=lambda conc, people, uby, ppp: (conc, people, uby, ppp),
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        print(f"✅ [STEP 1] Excels leídos en {time.perf_counter() - t0:.3f}s")

        # 2️⃣ Calcular semana
        today = datetime.date.today()
        year = year or today.year
        week = week or today.isocalendar()[1]
        monday_curr = datetime.date.fromisocalendar(year, week, 1)
        sunday_curr = monday_curr + timedelta(days=6)
        monday_prev = monday_curr - timedelta(days=7)
        print(f"🗓️ [STEP 2] Semana {week}-{year} | Lunes {monday_curr} | Domingo {sunday_curr} | Límite purge anterior: {monday_prev}")

        # 3️⃣ Purga histórica
        session.exec(delete(Schedule).where(Schedule.start_date < monday_prev))
        session.exec(delete(UbycallSchedule).where(UbycallSchedule.date < monday_prev))

        # 4️⃣ Purga histórica de duplicados
        duplicated_schedules = session.exec(
            select(Schedule.worker_document, Schedule.start_date)
            .group_by(Schedule.worker_document, Schedule.start_date)
            .having(func.count(Schedule.id) > 1)
        ).all()

        for doc, date in duplicated_schedules:
            duplicate_records = session.exec(
                select(Schedule).where(
                    and_(Schedule.worker_document == doc, Schedule.start_date == date)
                )
            ).all()

            duplicate_records_sorted = sorted(duplicate_records, key=lambda x: x.id)
            records_to_delete = duplicate_records_sorted[:-1]

            if records_to_delete:
                for record in records_to_delete:
                    session.delete(record)

        session.commit()

        # 5️⃣ Procesar dataframes
        df_conc = schedule_concentrix(df_conc_raw, people_obs_raw, week=week, year=year)
        df_ppp = schedule_ppp(df_ppp_raw)
        df_conc = merge_schedule_concentrix(df_conc, df_ppp)
        df_uby = schedule_ubycall(df_uby_raw)
        print(f"📊 [STEP 4] df_conc={len(df_conc)} filas | df_uby={len(df_uby)} filas")

        # 6️⃣ Calcular documentos existentes
        conc_docs = set(df_conc[DOCUMENT].astype(str).str.strip())
        uby_docs = set(df_uby[DOCUMENT].astype(str).str.strip())
        all_docs = conc_docs.union(uby_docs)
        existing_docs = set(session.exec(select(Worker.document).where(Worker.document.in_(all_docs))).all())
        missing_docs = sorted(all_docs - existing_docs)
        print(f"🔍 [STEP 5] Docs totales: {len(all_docs)} | En Worker: {len(existing_docs)} | Faltantes: {len(missing_docs)}")

        # 7️⃣ Comparar y construir (solo nuevos)
        conc_records, uby_records = [], []
        inserted_conc = inserted_uby = 0

        # Concentrix: solo insertamos si no existe
        for row in df_conc.itertuples(index=False):
            doc = str(row.document).strip()
            
            key = (doc, row.start_date)

            existing_schedule = session.exec(
                select(Schedule).where(
                    and_(Schedule.worker_document == doc, Schedule.start_date == row.start_date)
                )
            ).first()

            if existing_schedule:
                if doc == '75350625':
                    print(existing_schedule)
                    print(row)
                existing_schedule.start_time = row.start_time
                existing_schedule.end_time = row.end_time
                existing_schedule.break_start = getattr(row, "break_start", None)
                existing_schedule.break_end = getattr(row, "break_end", None)
                existing_schedule.is_rest_day = bool(getattr(row, "rest_day", False))
                existing_schedule.obs = getattr(row, "obs", None)
                if doc == '75350625':
                    print('xd')
                    print(existing_schedule)
                session.add(existing_schedule)  # Merge actualiza el objeto en la base de datos
            else:
                # Añadir la fila solo si no se ha agregado previamente
                if key not in [(str(r['worker_document']), r['start_date']) for r in conc_records]:
                    conc_records.append({
                        "worker_document": doc,
                        "start_date": row.start_date,
                        "end_date": row.end_date,
                        "start_time": row.start_time,
                        "end_time": row.end_time,
                        "break_start": getattr(row, "break_start", None),
                        "break_end": getattr(row, "break_end", None),
                        "is_rest_day": bool(getattr(row, "rest_day", False)),
                        "obs": getattr(row, "obs", None)
                    })
                else:
                    continue

        # Ubycall: solo insertamos si no existe
        for row in df_uby.itertuples(index=False):
            doc = str(row.document).strip()
            
            if doc not in existing_docs:
                continue
            key = (doc, row.date, row.start_time, row.end_time)

            existing_uby = session.exec(
                select(UbycallSchedule).where(
                    and_(UbycallSchedule.worker_document == doc, 
                        UbycallSchedule.date == row.date,
                        UbycallSchedule.start_time == row.start_time,
                        UbycallSchedule.end_time == row.end_time)
                )
            ).first()

            if existing_uby:
                continue
            else:
                # Añadir la fila solo si no se ha agregado previamente
                if key not in [(str(r['worker_document']), r['date'], r['start_time'], r['end_time']) for r in uby_records]:
                    uby_records.append({
                        "worker_document": doc,
                        "date": row.date,
                        "day": row.day,
                        "start_time": row.start_time,
                        "end_time": row.end_time
                    })
                else:
                    continue

        print(f"🧱 [STEP 6] Registros nuevos detectados | Concentrix: {len(conc_records)} | Ubycall: {len(uby_records)}")

        # 8️⃣ Inserciones por lotes (solo si hay registros nuevos)
        if conc_records or uby_records:
            for batch in chunked(conc_records, 2000):
                session.bulk_insert_mappings(Schedule, batch)
                inserted_conc += len(batch)
            for batch in chunked(uby_records, 2000):
                session.bulk_insert_mappings(UbycallSchedule, batch)
                inserted_uby += len(batch)
            print(f"✅ [STEP 7] Bulk insert completado + commit")
        session.flush()
        session.commit()

        print(f"🏁 [SCHEDULES] OK | Concentrix insertados: {inserted_conc} | Ubycall insertados: {inserted_uby} | Missing: {len(missing_docs)} | Tiempo total: {time.perf_counter() - start_total:.3f}s\n")

        return {
            "inserted_concentrix": inserted_conc,
            "inserted_ubycall": inserted_uby,
            "missing_workers": missing_docs
        }

    except Exception as e:
        session.rollback()
        print("❌ [SCHEDULES] Error inesperado en process_and_persist_schedules:")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
    finally:
        session.autoflush = old_autoflush
        session.expire_on_commit = old_expire_on_commit
