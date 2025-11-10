from typing import List, Dict
from datetime import date
from fastapi import APIRouter, UploadFile, Depends, File, Form
from sqlmodel import Session, select
import traceback

from app.database.database import get_session
from app.services.attendance_service import process_and_persist_attendance
from app.models.worker import Attendance, Schedule, Worker

# Configurar logging básico
router = APIRouter(tags=["attendance"])


@router.post("/upload-attendance/", summary="Carga y persiste asistencia desde Excel")
async def upload_attendance(
    file: UploadFile = File(...),
    target_date: date | None = Form(None),
    session: Session = Depends(get_session),
):
    """
    - Recibe un Excel con conexiones (HeroCare u otro).
    - Procesa los check_in / check_out por agente y fecha.
    - Valida contra los schedules cargados para asignar status (present, absent, late).
    - Inserta registros de Attendance vinculados a Worker.
    """
    
    try:

        result = await process_and_persist_attendance(
            file=file,
            session=session,
            target_date=target_date
        )

        return {
            "message": f"Se insertaron {result['inserted']} registros de asistencia.",
            "missing_workers": result["missing_workers"]
        }

    except Exception as e:
        import traceback
        print("❌ ERROR EN ENDPOINT:")
        print(traceback.format_exc())
        raise e 


@router.get("/attendance/today", summary="Obtiene la asistencia de hoy")
def read_today_attendance(session: Session = Depends(get_session)) -> Dict[str, List]:
    """
    Devuelve la asistencia de hoy:
    - Si el worker tiene schedule pero aún no tiene Attendance registrado → status="absent".
    - Si ya tiene Attendance → devolver el registro real.
    """
    today = date.today()

    try:
        # 1. Schedules de hoy
        schedules_today = session.exec(
            select(Schedule).where(Schedule.date == today)
        ).all()

        # 2. Attendances de hoy
        attendance_today = session.exec(
            select(Attendance).where(Attendance.date == today)
        ).all()

        attendance_map = {
            (a.worker_email, a.date): a for a in attendance_today
        }

        results = []

        for sched in schedules_today:
            try:
                worker = session.exec(
                    select(Worker).where(Worker.document == sched.worker_document)
                ).first()

                if not worker:
                    continue  # skip si no existe el worker

                key = (worker.api_email, today)
                if key in attendance_map:
                    results.append(attendance_map[key])
                else:
                    # Crear registro "virtual" absent
                    absent_record = Attendance(
                        worker_email=worker.api_email,
                        date=today,
                        status="absent",
                        check_in=None,
                        check_out=None,
                        notes="No se ha registrado asistencia aún."
                    )
                    results.append(absent_record)

            except Exception as e_inner:
                print(f"❌ Error procesando schedule {sched.id}: {e_inner}")
                print(traceback.format_exc())

        return {"attendance": results}

    except Exception as e:
        print("❌ Error general en read_today_attendance")
        print(traceback.format_exc())
        return {"error": str(e)}
