import pandas as pd
from fastapi import HTTPException, UploadFile
from datetime import date, timedelta, datetime
from sqlmodel import Session, select, delete, and_
import traceback
import time as time_mod
import io
from sqlalchemy import text

from app.services.utils.upload_service import handle_file_upload_generic
from app.utils.validators.validate_excel_attendance import validate_excel_attendance
from app.core.workers_attendance.attendance import clean_attendance
from app.models.worker import Worker, Schedule, Attendance


async def process_and_persist_attendance(
    file: UploadFile,
    session: Session,
    target_date: date | None = None,
) -> dict:

    start_total = time_mod.perf_counter()
    print("\n📂 [ATT] Iniciando process_and_persist_attendance (optimizado)")

    try:
        # 1️⃣ Leer Excel validado
        t1 = time_mod.perf_counter()
        try:
            df_raw, = await handle_file_upload_generic(
                files=[file],
                validator=validate_excel_attendance,
                keyword_to_slot={"attendance": "attendance"},
                required_slots=["attendance"],
                post_process=lambda att: (att,),
            )
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=400, detail=f"Error al leer archivo: {e}")
        print(f"✅ [ATT-STEP 1] Lectura completada en {time_mod.perf_counter() - t1:.3f}s")

        # 2️⃣ Limpieza
        t2 = time_mod.perf_counter()
        try:
            df_attendance = clean_attendance(df_raw, target_date)
            if df_attendance.empty:
                raise HTTPException(status_code=400, detail="No se encontraron registros de asistencia")
        except HTTPException:
            raise
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=400, detail=f"Error al limpiar data: {e}")
        print(f"✅ [ATT-STEP 2] Limpieza completada en {time_mod.perf_counter() - t2:.3f}s")
        print(f"ℹ️ [ATT] Registros procesados: {len(df_attendance)}")

        # 3️⃣ target_date
        if not target_date:
            target_date = df_attendance["date"].iloc[0]
        print(f"📅 [ATT-STEP 3] target_date={target_date}")

        # 4️⃣ DELETE optimizado (RAW SQL → ~100ms)
        t4 = time_mod.perf_counter()
        session.exec(
            text("DELETE FROM attendance WHERE date = :d").bindparams(d=target_date)
        )
        session.commit()
        print(f"🧹 [ATT-STEP 4] Purga completada en {time_mod.perf_counter() - t4:.3f}s")

        # 5️⃣ Precarga
        t5a = time_mod.perf_counter()

        all_emails = df_attendance["api_email"].astype(str).str.strip().unique().tolist()
        workers = session.exec(select(Worker).where(Worker.api_email.in_(all_emails))).all()
        worker_map = {w.api_email.strip(): w for w in workers}
        print(f"👥 [ATT-STEP 5.1] Workers cargados: {len(worker_map)}")

        all_docs = [w.document for w in workers]
        schedules = session.exec(
            select(Schedule).where(
                and_(Schedule.start_date == target_date, Schedule.worker_document.in_(all_docs))
            )
        ).all()
        schedule_map = {(s.worker_document, s.start_date): s for s in schedules}
        print(f"🗓️ [ATT-STEP 5.2] Schedules cargados: {len(schedule_map)} | Tiempo precarga: {time_mod.perf_counter() - t5a:.3f}s")

        # 6️⃣ Loop principal
        t6 = time_mod.perf_counter()

        records = []
        inserted = 0
        missing_workers = []
        no_schedule = 0
        invalid_schedule = 0
        now = datetime.now()

        for row in df_attendance.itertuples(index=False):
            api_email = str(row.api_email).strip()
            date_row = row.date
            check_in_times = row.check_in_times
            check_out_times = row.check_out_times

            worker = worker_map.get(api_email)
            if not worker:
                missing_workers.append(api_email)
                continue

            schedule = schedule_map.get((worker.document, date_row))
            if not schedule:
                no_schedule += 1
                continue
            if schedule.start_time is None or schedule.end_time is None:
                invalid_schedule += 1
                continue

            start_dt = datetime.combine(date_row, schedule.start_time)
            end_dt = datetime.combine(date_row, schedule.end_time)

            total_out = 0
            total_off = 0
            status = "Absent"
            check_in = check_out = None

            # CHECK-IN
            valid_ci = [
                ci for ci in check_in_times
                if datetime.combine(date_row, ci[0]) >= start_dt - timedelta(hours=3)
            ]
            valid_ci.sort()

            if valid_ci:
                check_in = valid_ci[0]
                ci_time = datetime.combine(date_row, check_in[0])

                if ci_time <= start_dt + timedelta(minutes=10):
                    status = "Present"
                elif ci_time > start_dt + timedelta(minutes=10):
                    status = "Late"

                for ci in valid_ci:
                    ci_time = datetime.combine(date_row, ci[0])
                    ci_end = ci_time + timedelta(minutes=ci[1])
                    if ci_end > end_dt:
                        cutoff = min(ci_end, end_dt + timedelta(hours=3))
                        total_out += max(0, round((cutoff - max(ci_time, end_dt)).total_seconds() / 60))

            # CHECK-OUT
            valid_co = [
                co for co in check_out_times
                if start_dt < datetime.combine(date_row, co[0]) <= end_dt + timedelta(hours=4)
            ]
            valid_co.sort()

            if valid_co:
                window_start = end_dt - timedelta(minutes=5)
                near_end = [
                    co for co in valid_co
                    if window_start <= datetime.combine(date_row, co[0]) <= end_dt
                ]
                check_out = near_end[0] if near_end else valid_co[-1]

            if now < end_dt:
                check_out = None
                
            if api_email == 'eajuarez.whl@service.glovoapp.com':
                print(now)
                print('xdxd')
                print(end_dt)
                print('xdxd')
            for co in valid_co:
                co_time = datetime.combine(date_row, co[0])
                co_end = co_time + timedelta(minutes=co[1])
                if co_time <= end_dt:
                    total_off += max(0, round((min(co_end, end_dt) - co_time).total_seconds() / 60))

            if valid_ci:
                records.append((
                    api_email,
                    date_row,
                    check_in[0],
                    check_out[0] if check_out else None,
                    status,
                    total_out,
                    total_off,
                ))
                inserted += 1

        loop_time = time_mod.perf_counter() - t6
        print(
            f"✅ [ATT-STEP 6] Loop completado en {loop_time:.3f}s | "
            f"Insertados={inserted} | MissingWorkers={len(missing_workers)} | "
            f"SinSchedule={no_schedule} | ScheduleInválido={invalid_schedule}"
        )

        # 7️⃣ COPY ultra rápido (0.1s – 0.3s)
        t7 = time_mod.perf_counter()

        if records:
            buffer = io.StringIO()
            for r in records:
                co_value = r[3] if r[3] else "\\N"
                buffer.write(
                    f"{r[0]}\t{r[1]}\t{r[2]}\t{co_value}\t{r[4]}\t{r[5]}\t{r[6]}\n"
                )

            buffer.seek(0)

            conn = session.connection().connection
            cursor = conn.cursor()
            cursor.copy_from(
                file=buffer,
                table="attendance",
                columns=("api_email", "date", "check_in", "check_out", "status", "out_of_adherence", "offline_minutes"),
                sep="\t",
                null="\\N",
            )
            conn.commit()
            cursor.close()

        print(f"💾 [ATT-STEP 7] COPY insert completado en {time_mod.perf_counter() - t7:.3f}s")

        total_time = time_mod.perf_counter() - start_total
        print(f"🏁 [ATT] Proceso completo OK | Insertados={inserted} | Tiempo total={total_time:.3f}s\n")

        return {
            "inserted": inserted,
            "missing_workers": missing_workers,
        }

    except HTTPException:
        raise
    except Exception as e:
        session.rollback()
        print("❌ [ATT] Error inesperado en process_and_persist_attendance:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
