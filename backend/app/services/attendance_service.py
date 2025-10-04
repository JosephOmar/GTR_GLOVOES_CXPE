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
    print(df_attendance[df_attendance['kustomer_email'] == 'taflores.whl@service.glovoapp.com'])
    print(df_attendance[df_attendance['kustomer_email'] == 'jquinte.whl@service.glovoapp.com'])
    # 5) Iterar sobre los registros limpios
    for row in df_attendance.itertuples(index=False):
        kustomer_email = str(row._asdict()["kustomer_email"]).strip()
        check_in = row._asdict()["check_in"]
        check_out = row._asdict()["check_out"]
        date_row = row._asdict()["date"]

        # Buscar worker por email
        worker = session.exec(
            select(Worker).where(Worker.kustomer_email == kustomer_email)
        ).first()

        if not worker:
            missing_workers.append(kustomer_email)
            continue

        # Buscar schedule para ese worker en la fecha
        schedule = session.exec(
            select(Schedule).where(
                and_(
                    Schedule.worker_document == worker.document,
                    Schedule.date == date_row
                )
            )
        ).first()

        # Status por defecto = Absent
        status = "Absent"

        if schedule and schedule.start_time and check_in:
            # Normalizar check_in a datetime.time
            if isinstance(check_in, pd.Timestamp):
                check_in_time = check_in.time().replace(microsecond=0)
            elif isinstance(check_in, datetime):
                check_in_time = check_in.time().replace(microsecond=0)
            else:
                check_in_time = check_in.replace(microsecond=0)

            # Tolerance time
            tolerance_datetime = datetime.combine(date_row, schedule.start_time) + timedelta(minutes=5)
            tolerance_time = tolerance_datetime.time().replace(microsecond=0)

            if check_in_time <= tolerance_time:
                status = "Present"
            else:
                status = "Late"
        else:
            # Si no hay schedule o start_time, lo dejamos como Absent
            status = "Absent"

        # Crear registro de asistencia
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
