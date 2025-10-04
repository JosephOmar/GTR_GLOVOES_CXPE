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

    # 5) Iterar sobre los registros limpios
    # Iterar sobre todos los trabajadores
    for row in df_attendance.itertuples(index=False):
        kustomer_email = str(row._asdict()["kustomer_email"]).strip()
        check_in = row._asdict()["check_in"]
        check_out = row._asdict()["check_out"]
        date_row = row._asdict()["date"]

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

        # Por defecto = Absent
        status = "Absent"

        if schedule:
            if check_in:
                tolerance_time = (datetime.combine(date_row, schedule.start_time) + timedelta(minutes=5)).time()
                if check_in <= tolerance_time:
                    status = "Present"
                else:
                    status = "Late"
            else:
                # Si hay schedule pero no check_in → sigue siendo Absent
                status = "Absent"

            # Guardar registro aunque no haya check_in
            attendance = Attendance(
                kustomer_email=worker.kustomer_email,
                date=date_row,
                check_in=check_in,
                check_out=check_out,
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
