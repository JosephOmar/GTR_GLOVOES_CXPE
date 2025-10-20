# app/services/attendance_service.py

import pandas as pd
from fastapi import HTTPException, UploadFile
from typing import List
from datetime import date, timedelta, datetime, time
from sqlmodel import Session, select, delete, and_
import traceback

from app.services.utils.upload_service import handle_file_upload_generic
from app.utils.validators.validate_excel_attendance import validate_excel_attendance
from app.core.workers_attendance.attendance import clean_attendance
from app.models.worker import Worker, Schedule, Attendance


async def process_and_persist_attendance(
    file: UploadFile,
    session: Session,
    target_date: date | None = None,
) -> dict:
    """
    Procesa archivo de asistencia, limpia los datos, calcula el estado (present, late, absent),
    determina check_in, check_out, el tiempo fuera de adherencia (out_of_adherence)
    y el tiempo total desconectado dentro del turno (offline_minutes).
    """

    try:
        # 1️⃣ Leer Excel validado
        df_raw, = await handle_file_upload_generic(
            files=[file],
            validator=validate_excel_attendance,
            keyword_to_slot={"attendance": "attendance"},
            required_slots=["attendance"],
            post_process=lambda att: (att,)
        )
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"Error al leer archivo: {e}")

    # 2️⃣ Normalizar data
    try:
        df_attendance = clean_attendance(df_raw, target_date)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail="Error al limpiar la data de asistencia")

    if df_attendance.empty:
        raise HTTPException(status_code=400, detail="No se encontraron registros de asistencia")

    # 3️⃣ Determinar fecha objetivo
    if not target_date:
        target_date = df_attendance["date"].iloc[0]

    # 4️⃣ Eliminar registros previos
    session.exec(delete(Attendance).where(Attendance.date == target_date))

    inserted = 0
    missing_workers = []

    # 5️⃣ Iterar sobre registros
    for idx, row in enumerate(df_attendance.itertuples(index=False)):
        try:
            api_email = str(row.api_email).strip()
            date_row = row.date

            worker = session.exec(
                select(Worker).where(Worker.api_email == api_email)
            ).first()

            if not worker:
                missing_workers.append(api_email)
                continue

            schedule = session.exec(
                select(Schedule).where(
                    and_(
                        Schedule.worker_document == worker.document,
                        Schedule.date == date_row
                    )
                )
            ).first()

            if not schedule or not schedule.start_time or not schedule.end_time:
                continue

            start_dt = datetime.combine(date_row, schedule.start_time)
            end_dt = datetime.combine(date_row, schedule.end_time)
            crosses_midnight = schedule.end_time < schedule.start_time
            if crosses_midnight:
                end_dt += timedelta(days=1)

            # ✅ Crear variables filtradas (sin modificar row)
            checkins = sorted([
                t for t in getattr(row, "check_in_times", []) or []
                if schedule.start_time <= t <= (time(23, 59) if not crosses_midnight else time(12, 0))
            ])

            checkouts = sorted([
                t for t in getattr(row, "check_out_times", []) or []
                if schedule.start_time <= t <= (time(23, 59) if not crosses_midnight else time(12, 0))
            ])

            # 🔹 Calcular check_in
            check_in = None
            if checkins:
                check_in = min(
                    checkins,
                    key=lambda t: abs((datetime.combine(date_row, t) - start_dt).total_seconds())
                )

            # 🔹 Calcular check_out
            check_out = None
            if checkouts and check_in:
                check_in_dt = datetime.combine(date_row, check_in)
                possible_checkouts = [
                    t for t in checkouts
                    if datetime.combine(date_row, t) > check_in_dt
                ]

                if possible_checkouts:
                    # Buscar checkout más cercano al end_time (preferido)
                    closest_to_end = min(
                        possible_checkouts,
                        key=lambda t: abs((datetime.combine(date_row, t) - end_dt).total_seconds())
                    )

                    # ✅ Nuevo criterio más flexible:
                    delta_sec = (datetime.combine(date_row, closest_to_end) - end_dt).total_seconds()
                    if -10 * 60 <= delta_sec <= 15 * 60:  # acepta entre 10 min antes y 15 después del fin
                        check_out = closest_to_end
                    else:
                        valid_within_shift = [
                            t for t in possible_checkouts
                            if datetime.combine(date_row, t) <= end_dt
                        ]
                        check_out = max(valid_within_shift or possible_checkouts)

            # 🔹 Determinar status y adherencia
            status = "Absent"
            out_of_adherence = None
            offline_minutes = 0

            if check_in:
                check_in_dt = datetime.combine(date_row, check_in)
                earliest_valid_checkin = start_dt - timedelta(hours=3)
                if earliest_valid_checkin <= check_in_dt <= end_dt:
                    tolerance_dt = start_dt + timedelta(minutes=10)
                    status = "Present" if check_in_dt <= tolerance_dt else "Late"
                else:
                    status = "Absent"
                    check_in = None

            # ==========================
            # 🔹 Calcular tiempo desconectado dentro del turno (offline_minutes)
            # ==========================
            ins = sorted(checkins)
            outs = sorted(checkouts)
            offline_minutes = 0

            # 1️⃣ Si no hay conexión en todo el turno
            if not ins:
                offline_minutes = int(round((end_dt - start_dt).total_seconds() / 60))
            else:
                # 2️⃣ Si llegó tarde (primer online después del inicio del turno)
                first_in_dt = datetime.combine(date_row, ins[0])
                if first_in_dt > start_dt:
                    offline_minutes += (first_in_dt - start_dt).total_seconds() / 60

                # 3️⃣ Desconexiones intermedias (entre un offline y el siguiente online)
                for i in range(len(outs)):
                    if i + 1 < len(ins):
                        out_dt = datetime.combine(date_row, outs[i])
                        next_in_dt = datetime.combine(date_row, ins[i + 1])
                        diff = (next_in_dt - out_dt).total_seconds() / 60
                        if diff > 0 and start_dt <= out_dt <= end_dt:
                            offline_minutes += diff

                # 4️⃣ Si se desconectó antes del fin del turno y no volvió a conectarse
                if outs:
                    last_out_dt = datetime.combine(date_row, outs[-1])
                    if last_out_dt < end_dt:
                        offline_minutes += (end_dt - last_out_dt).total_seconds() / 60

            offline_minutes = int(round(offline_minutes))

            # 🔹 Calcular fuera de adherencia (solo si se quedó más del fin)
            if check_out:
                check_out_dt = datetime.combine(date_row, check_out)
                if crosses_midnight and check_out < schedule.end_time:
                    check_out_dt += timedelta(days=1)
                if check_out_dt > end_dt:
                    out_of_adherence = int(round((check_out_dt - end_dt).total_seconds() / 60))

            # 🔹 Registrar en BD
            attendance = Attendance(
                api_email=worker.api_email,
                date=date_row,
                check_in=check_in,
                check_out=check_out,
                status=status,
                offline_minutes=offline_minutes,
                out_of_adherence=out_of_adherence,
            )
            session.add(attendance)
            inserted += 1

        except Exception as e:
            traceback.print_exc()
            continue

    session.commit()

    print(f"\n✅ Procesamiento finalizado: {inserted} registros insertados.")

    return {
        "inserted": inserted,
        "missing_workers": missing_workers,
        "date": str(target_date)
    }
