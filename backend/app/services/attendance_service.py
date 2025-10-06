# app/services/attendance_service.py

import pandas as pd
from fastapi import HTTPException, UploadFile
from typing import List
from datetime import date, timedelta, datetime
from sqlmodel import Session, select, delete, and_

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
    Procesa archivos de asistencia, limpia la data y persiste en DB con status calculado.
    """

    # 1) Leer Excel(s) de HeroCare
    try:
        df_raw, = await handle_file_upload_generic(
            files=[file],
            validator=validate_excel_attendance,
            keyword_to_slot={"attendance": "attendance"},
            required_slots=["attendance"],
            post_process=lambda att: (att,)
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 2) Normalizar con clean_attendance
    df_attendance = clean_attendance(df_raw)

    if df_attendance.empty:
        raise HTTPException(status_code=400, detail="No se encontraron registros de asistencia")

    # 3) Si no se pasa fecha, tomamos la de la data (primer registro)
    if not target_date:
        target_date = df_attendance["date"].iloc[0]

    # 4) Purga asistencias de ese mismo día (evita duplicados por nueva carga)
    session.exec(
        delete(Attendance).where(Attendance.date == target_date)
    )

    inserted = 0
    missing_workers = []

    for row in df_attendance.itertuples(index=False):
        kustomer_email = str(row.kustomer_email).strip()
        date_row = row.date

        worker = session.exec(
            select(Worker).where(Worker.kustomer_email == kustomer_email)
        ).first()

        if not worker:
            missing_workers.append(kustomer_email)
            continue

        schedule = session.exec(
            select(Schedule).where(
                and_(
                    Schedule.worker_document == worker.document,
                    Schedule.date == date_row
                )
            )
        ).first()

        print(f"\n--- Procesando {kustomer_email} ---")
        if schedule:
            print(f"Horario: {schedule.start_time} - {schedule.end_time}")
        else:
            print("⚠️ No se encontró schedule para este trabajador")

        check_in = None
        if schedule and schedule.start_time and hasattr(row, "check_in_times"):
            possible_checkins = row.check_in_times
            if possible_checkins:
                start_dt = datetime.combine(date_row, schedule.start_time)
                check_in = min(
                    possible_checkins,
                    key=lambda t: abs((datetime.combine(date_row, t) - start_dt).total_seconds())
                )
                print(f"Posibles check-ins: {possible_checkins}")
                print(f"Check-in elegido: {check_in}")
            else:
                print("⚠️ Sin check-ins disponibles")

        if schedule and schedule.start_time and check_in:
            # Normalizar check_in
            check_in_dt = datetime.combine(date_row, check_in)

            # Calcular inicio y fin del turno
            start_dt = datetime.combine(date_row, schedule.start_time)
            end_dt = datetime.combine(date_row, schedule.end_time)
            if schedule.end_time < schedule.start_time:
                end_dt += timedelta(days=1)

            earliest_valid_checkin = start_dt - timedelta(minutes=125)

            if earliest_valid_checkin <= check_in_dt <= end_dt:
                tolerance_dt = start_dt + timedelta(minutes=10)
                status = "Present" if check_in_dt <= tolerance_dt else "Late"
            else:
                status = "Absent"
                check_in = None
        else:
            status = "Absent"
            check_in = None

        attendance = Attendance(
            kustomer_email=worker.kustomer_email,
            date=date_row,
            check_in=check_in,
            check_out=None,
            status=status,
        )
        session.add(attendance)
        inserted += 1

    session.commit()

    return {
        "inserted": inserted,
        "missing_workers": missing_workers,
        "date": str(target_date)
    }
