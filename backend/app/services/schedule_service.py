import pandas as pd
from fastapi import HTTPException, UploadFile
from typing import List
from datetime import date, timedelta
from sqlmodel import Session, select, delete, and_, func
import time  # ✅ para medir duración
import traceback
import datetime

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
    import time
    import traceback

    def chunked(iterable, size=2000):
        for i in range(0, len(iterable), size):
            yield iterable[i:i + size]

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
            df_conc_raw, people_obs_raw, df_uby_raw = await handle_file_upload_generic(
                files=files,
                validator=validate_excel_schedule,
                keyword_to_slot={
                    "schedule_concentrix": "schedule_concentrix",
                    "people_obs": "people_obs",
                    "schedule_ubycall": "schedule_ubycall",
                },
                required_slots=["schedule_concentrix", "people_obs", "schedule_ubycall"],
                post_process=lambda conc, people, uby: (conc, people, uby),
            )
        except ValueError as e:
            print(f"⛔ [STEP 1] Error al leer/validar excels: {e}")
            raise HTTPException(status_code=400, detail=str(e))
        print(f"✅ [STEP 1] Excels leídos en {time.perf_counter() - t0:.3f}s")

        # 2️⃣ Calcular semana
        t1 = time.perf_counter()
        today = datetime.date.today()
        year = year or today.year
        week = week or today.isocalendar()[1]
        monday_curr = datetime.date.fromisocalendar(year, week, 1)
        sunday_curr = monday_curr + timedelta(days=6)
        monday_prev = monday_curr - timedelta(days=7)
        print(f"🗓️ [STEP 2] Semana {week}-{year} | Lunes {monday_curr} | Domingo {sunday_curr} | Límite purge anterior: {monday_prev} | Tiempo {time.perf_counter() - t1:.3f}s")

        # 3️⃣ Purga histórica
        t2 = time.perf_counter()
        session.exec(delete(Schedule).where(Schedule.date < monday_prev))
        session.exec(delete(UbycallSchedule).where(UbycallSchedule.date < monday_prev))
        print(f"🧹 [STEP 3] Purga de históricos completada en {time.perf_counter() - t2:.3f}s")

        # 4️⃣ Purga histórica de duplicados
        t2 = time.perf_counter()

        # Identificar duplicados en la base de datos para Schedule
        duplicated_schedules = session.exec(
            select(Schedule.worker_document, Schedule.date)
            .group_by(Schedule.worker_document, Schedule.date)
            .having(func.count(Schedule.id) > 1)  # Usamos func.count() para contar las filas
        ).all()

        print(f"🔍 Duplicados encontrados: {len(duplicated_schedules)}")

        # Si hay duplicados, procesarlos en lotes para mejorar el rendimiento
        for doc, date in duplicated_schedules:
            if doc == '40935543':  # Solo imprimir si el worker_document es '40935543'
                print(f"🔍 Eliminando duplicados para worker_document={doc} en fecha={date}")

            # Obtener los registros duplicados para ese worker_document y fecha
            duplicate_records = session.exec(
                select(Schedule).where(
                    and_(Schedule.worker_document == doc, Schedule.date == date)
                )
            ).all()

            # Ordenamos los registros por 'id' para determinar cuál mantener (mantener el último)
            duplicate_records_sorted = sorted(duplicate_records, key=lambda x: x.id)

            # Eliminar duplicados en lotes
            # Mantener el último registro y eliminar los demás
            records_to_delete = duplicate_records_sorted[:-1]

            # Usamos session.delete() para eliminar los registros duplicados
            if records_to_delete:
                print(f"🗑️ Eliminando {len(records_to_delete)} registros duplicados para worker_document={doc} en fecha={date}")
                for record in records_to_delete:
                    session.delete(record)

        # Hacer commit de las eliminaciones
        session.commit()

        print(f"🧹 [STEP 3] Purga de duplicados completada en {time.perf_counter() - t2:.3f}s")

        # 5️⃣ Procesar dataframes
        t3 = time.perf_counter()
        df_conc = schedule_concentrix(df_conc_raw, people_obs_raw, week=week, year=year)
        df_uby = schedule_ubycall(df_uby_raw)
        print(f"📊 [STEP 4] df_conc={len(df_conc)} filas | df_uby={len(df_uby)} filas | Tiempo {time.perf_counter() - t3:.3f}s")

        # 6️⃣ Calcular documentos existentes
        t4 = time.perf_counter()
        conc_docs = set(df_conc[DOCUMENT].astype(str).str.strip())
        uby_docs = set(df_uby[DOCUMENT].astype(str).str.strip())
        all_docs = conc_docs.union(uby_docs)
        existing_docs = set(session.exec(select(Worker.document).where(Worker.document.in_(all_docs))).all())
        missing_docs = sorted(all_docs - existing_docs)
        print(f"🔍 [STEP 5] Docs totales: {len(all_docs)} | En Worker: {len(existing_docs)} | Faltantes: {len(missing_docs)} | Tiempo {time.perf_counter() - t4:.3f}s")

        # 6.1️⃣ Registros existentes
        t5a = time.perf_counter()
        existing_schedules = session.exec(
            select(Schedule.worker_document, Schedule.date, Schedule.start_time, Schedule.end_time,
                   Schedule.break_start, Schedule.break_end, Schedule.is_rest_day, Schedule.obs)
            .where(and_(Schedule.date >= monday_curr, Schedule.date <= sunday_curr))
        ).all()
        existing_ubycall = session.exec(
            select(UbycallSchedule.worker_document, UbycallSchedule.date, UbycallSchedule.start_time, UbycallSchedule.end_time)
            .where(and_(UbycallSchedule.date >= monday_curr, UbycallSchedule.date <= sunday_curr))
        ).all()

        # Mapa Concentrix (clave simple)
        existing_conc_map = {
            (str(doc).strip(), d): (st, et, bs, be, bool(rest), obs)
            for doc, d, st, et, bs, be, rest, obs in existing_schedules
        }

        # Mapa Ubycall (clave extendida)
        existing_uby_map = {
            (str(doc).strip(), d, st, et): True
            for doc, d, st, et in existing_ubycall
        }

        print(f"📊 [STEP 5.1] Registros existentes semana actual | Concentrix={len(existing_conc_map)} | Ubycall={len(existing_uby_map)} | Tiempo {time.perf_counter() - t5a:.3f}s")

        # 7️⃣ Comparar y construir (solo nuevos)
        t6 = time.perf_counter()
        conc_records, conc_keys_to_delete = [], set()
        uby_records = []
        inserted_conc = inserted_uby = 0

        # Comprobar si hay nuevos registros para procesar
        if df_conc.empty and df_uby.empty:
            print("⚠️ No hay nuevos registros para insertar. Saltando procesamiento de dataframes.")
        else:
            # Concentrix: sigue igual (clave simple)
            for row in df_conc.itertuples(index=False):
                doc = str(row.document).strip()
                if doc not in existing_docs:
                    continue
                key = (doc, row.date)

                # Obtener los valores de la nueva fila
                new_val = (
                    row.start_time, row.end_time,
                    getattr(row, "break_start", None),
                    getattr(row, "break_end", None),
                    bool(getattr(row, "rest_day", False)),
                    getattr(row, "obs", None)
                )

                # Buscar el valor existente
                old_val = existing_conc_map.get(key)

                # Función para manejar la comparación de valores nulos
                def compare_values(val1, val2):
                    if val1 is None and val2 is None:
                        return False  # Ambos son None, no hay cambio
                    return val1 != val2  # Los valores son diferentes, hay cambio

                # Comparar todos los campos. Si alguna diferencia existe, se actualiza el registro
                if old_val is None or any(compare_values(old, new) for old, new in zip(old_val, new_val)):
                    # Si hay diferencias, verificamos si debemos actualizar o insertar
                    if old_val is not None:
                        # Si el registro ya existe, actualizamos todos los campos
                        existing_schedules_update = session.exec(
                            select(Schedule).where(
                                and_(Schedule.worker_document == doc, Schedule.date == row.date)
                            )
                        ).first()
                        if existing_schedules_update:
                            # Actualizar todos los campos
                            existing_schedules_update.start_time = new_val[0]
                            existing_schedules_update.end_time = new_val[1]
                            existing_schedules_update.break_start = new_val[2]
                            existing_schedules_update.break_end = new_val[3]
                            existing_schedules_update.is_rest_day = new_val[4]
                            existing_schedules_update.obs = new_val[5]  # Solo actualizamos 'obs' si cambia
                            session.add(existing_schedules_update)  # Marcamos para actualizar
                    else:
                        # Si el registro no existe, insertamos un nuevo registro
                        conc_records.append({
                            "worker_document": doc,
                            "date": row.date,
                            "day": row.day,
                            "start_time": row.start_time,
                            "end_time": row.end_time,
                            "break_start": new_val[2],
                            "break_end": new_val[3],
                            "is_rest_day": new_val[4],
                            "obs": new_val[5]
                        })

            # Ubycall: clave extendida por bloque horario
            for row in df_uby.itertuples(index=False):
                doc = str(row.document).strip()
                if doc not in existing_docs:
                    continue
                key = (doc, row.date, row.start_time, row.end_time)
                if key not in existing_uby_map:
                    uby_records.append({
                        "worker_document": doc,
                        "date": row.date,
                        "day": row.day,
                        "start_time": row.start_time,
                        "end_time": row.end_time
                    })

        print(f"🧱 [STEP 6] Registros nuevos detectados | Concentrix: {len(conc_records)} | Ubycall: {len(uby_records)} | Tiempo {time.perf_counter() - t6:.3f}s")

        # 8️⃣ Inserciones por lotes (solo si hay registros nuevos)
        if conc_records or uby_records:
            t7 = time.perf_counter()
            for i, batch in enumerate(chunked(conc_records, 2000), 1):
                session.bulk_insert_mappings(Schedule, batch)
                inserted_conc += len(batch)
                print(f"💾 [STEP 7A] Lote Concentrix {i} insertado ({len(batch)} regs, total {inserted_conc})")

            for i, batch in enumerate(chunked(uby_records, 2000), 1):
                session.bulk_insert_mappings(UbycallSchedule, batch)
                inserted_uby += len(batch)
                print(f"💾 [STEP 7B] Lote Ubycall {i} insertado ({len(batch)} regs, total {inserted_uby})")

            session.commit()
            print(f"✅ [STEP 7] Bulk insert completado + commit en {time.perf_counter() - t7:.3f}s")
        else:
            print("🚫 No hay registros nuevos para insertar.")

        print(f"🏁 [SCHEDULES] OK | Concentrix insertados: {inserted_conc} | Ubycall insertados: {inserted_uby} | Missing: {len(missing_docs)} | Tiempo total: {time.perf_counter() - start_total:.3f}s\n")

        return {
            "inserted_concentrix": inserted_conc,
            "inserted_ubycall": inserted_uby,
            "missing_workers": missing_docs
        }

    except Exception as e:
        session.rollback()
        print("❌ [SCHEDULES] Error inesperado en process_and_persist_schedules:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
    finally:
        session.autoflush = old_autoflush
        session.expire_on_commit = old_expire_on_commit
