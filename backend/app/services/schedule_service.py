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
    df_ppp["document"] = df_ppp["document"].astype(str)

    cols_replace = ["start_time", "end_time", "break_start", "break_end", "rest_day"]

    df_merged = df_conc.merge(
        df_ppp[["document", "start_date", "end_date"] + cols_replace],
        on=["document", "start_date"],
        how="left",
        suffixes=("", "_ppp")
    )

    # Reemplazar solo cuando PPP tiene datos
    for col in cols_replace:
        df_merged[col] = df_merged[f"{col}_ppp"].combine_first(df_merged[col])

    # mantener end_date de conc si PPP no tiene
    df_merged["end_date"] = df_merged["end_date_ppp"].combine_first(df_merged["end_date"])

    # eliminar columnas auxiliares
    df_merged = df_merged.drop(columns=[f"{col}_ppp" for col in cols_replace] + ["end_date_ppp"])

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
        # Tiempo 1: Lectura de archivos
        t1 = time.perf_counter()
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
        t2 = time.perf_counter()
        print(f"⏳ Tiempo 1 (Lectura de archivos): {t2 - t1:.4f} segundos")

        # Tiempo 2: Cálculo de semana
        t3 = time.perf_counter()
        today = datetime.date.today()
        year = year or today.year
        week = week or today.isocalendar()[1]
        monday_curr = datetime.date.fromisocalendar(year, week, 1)
        sunday_curr = monday_curr + timedelta(days=6)
        monday_prev = monday_curr - timedelta(days=7)
        t4 = time.perf_counter()
        print(f"⏳ Tiempo 2 (Cálculo de semana): {t4 - t3:.4f} segundos")

        # Tiempo 3: Purga histórica
        t5 = time.perf_counter()
        session.exec(delete(Schedule).where(Schedule.start_date < monday_prev))
        session.exec(delete(UbycallSchedule).where(UbycallSchedule.date < monday_prev))
        t6 = time.perf_counter()
        print(f"⏳ Tiempo 3 (Purga histórica): {t6 - t5:.4f} segundos")

        # Tiempo 4: Purga histórica de duplicados
        t7 = time.perf_counter()
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
        t8 = time.perf_counter()
        print(f"⏳ Tiempo 4 (Purga histórica de duplicados): {t8 - t7:.4f} segundos")

        # Tiempo 5: Procesar dataframes
        t9 = time.perf_counter()
        df_conc = schedule_concentrix(df_conc_raw, people_obs_raw, week=week, year=year)
        df_ppp = schedule_ppp(df_ppp_raw)
        df_conc = merge_schedule_concentrix(df_conc, df_ppp)
        df_uby = schedule_ubycall(df_uby_raw)
        t10 = time.perf_counter()
        print(f"⏳ Tiempo 5 (Procesar dataframes): {t10 - t9:.4f} segundos")

        # Tiempo 6: Calcular documentos existentes
        t11 = time.perf_counter()
        conc_docs = set(df_conc[DOCUMENT].astype(str).str.strip())
        uby_docs = set(df_uby[DOCUMENT].astype(str).str.strip())
        all_docs = conc_docs.union(uby_docs)
        existing_docs = set(session.exec(select(Worker.document).where(Worker.document.in_(all_docs))).all())
        missing_docs = sorted(all_docs - existing_docs)
        t12 = time.perf_counter()
        print(f"⏳ Tiempo 6 (Calcular documentos existentes): {t12 - t11:.4f} segundos")

        # Tiempo 7: Comparar y construir (optimizado)
        t13 = time.perf_counter()

        conc_records = []
        uby_records = []
        inserted_conc = inserted_uby = 0

        # ============================================================
        # 1. Cargar TODOS los schedules existentes en memoria
        # ============================================================

        existing_conc = {
            (s.worker_document, s.start_date): s
            for s in session.exec(select(Schedule)).all()
        }

        existing_uby = {
            (s.worker_document, s.date, s.start_time, s.end_time): True
            for s in session.exec(select(UbycallSchedule)).all()
        }

        # ============================================================
        # 2. Procesar Concentrix en memoria (sin consultas en bucle)
        # ============================================================

        for row in df_conc.itertuples(index=False):
            doc = str(row.document).strip()
            key = (doc, row.start_date)

            existing = existing_conc.get(key)

            if existing:
                # UPDATE rápido sin queries
                changed = False

                # Campos directos
                fields = [
                    ("start_time", row.start_time),
                    ("end_time", row.end_time),
                    ("break_start", getattr(row, "break_start", None)),
                    ("break_end", getattr(row, "break_end", None)),
                    ("is_rest_day", bool(getattr(row, "rest_day", False))),
                    ("obs", getattr(row, "obs", None)),
                ]

                for attr, new_value in fields:
                    if getattr(existing, attr) != new_value:
                        setattr(existing, attr, new_value)
                        changed = True

                if changed:
                    session.add(existing)

            else:
                conc_records.append({
                    "worker_document": doc,
                    "start_date": row.start_date,
                    "end_date": row.end_date,
                    "start_time": row.start_time,
                    "end_time": row.end_time,
                    "break_start": getattr(row, "break_start", None),
                    "break_end": getattr(row, "break_end", None),
                    "is_rest_day": bool(getattr(row, "rest_day", False)),
                    "obs": getattr(row, "obs", None),
                })

        # ============================================================
        # 3. Procesar Ubycall (evita queries y duplicados)
        # ============================================================

        for row in df_uby.itertuples(index=False):
            doc = str(row.document).strip()

            if doc not in existing_docs:
                continue

            key = (doc, row.date, row.start_time, row.end_time)

            if key in existing_uby:
                continue  # Ya existe en DB

            uby_records.append({
                "worker_document": doc,
                "date": row.date,
                "day": row.day,
                "start_time": row.start_time,
                "end_time": row.end_time,
            })

        t14 = time.perf_counter()
        print(f"⏳ Tiempo 7 (Comparar y construir optimizado): {t14 - t13:.4f} segundos")


        # Tiempo 8: Inserciones por lotes
        t15 = time.perf_counter()
        if conc_records or uby_records:
            for batch in chunked(conc_records, 2000):
                session.bulk_insert_mappings(Schedule, batch)
                inserted_conc += len(batch)
            for batch in chunked(uby_records, 2000):
                session.bulk_insert_mappings(UbycallSchedule, batch)
                inserted_uby += len(batch)
        session.flush()
        session.commit()
        t16 = time.perf_counter()
        print(f"⏳ Tiempo 8 (Inserciones por lotes): {t16 - t15:.4f} segundos")

        # Tiempo total
        end_total = time.perf_counter()
        print(f"🕒 Tiempo total: {end_total - start_total:.4f} segundos")

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
