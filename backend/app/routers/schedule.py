from typing import List, Dict
from datetime import date
from fastapi import APIRouter, UploadFile, Depends, File, Form
from sqlmodel import Session, select

from app.database.database import get_session
from app.services.schedule_service import process_and_persist_schedules
from app.models.worker import Schedule, UbycallSchedule

router = APIRouter(tags=["schedules"])

@router.post("/upload-schedules/", summary="Carga y persiste horarios desde Excel")
async def upload_schedules(
    files: List[UploadFile] = File(...),
    week: int = Form(...),                 # 👈 obligatorio, viene del form-data
    year: int | None = Form(None),         # 👈 opcional, también del form-data
    session: Session = Depends(get_session),
):
    """
    - Recibe dos Excel (concentrix y ubycall).
    - Inserta sólo los schedules cuyos DOCUMENT existen en Worker.
    - Devuelve cuántos se insertaron y lista de documentos faltantes.
    """
    result = await process_and_persist_schedules(
        files=files,
        session=session,
        week=week,
        year=year,
    )
    return {
        "message": (
            f"Se insertaron {result['inserted_concentrix']} horarios Concentrix "
            f"y {result['inserted_ubycall']} horarios Ubycall."
        ),
        "missing_worker_documents": result["missing_workers"]
    }

@router.get(
    "/schedules/today",
    summary="Obtiene los horarios de hoy (Concentrix y Ubycall)"
)
def read_today_schedules(session: Session = Depends(get_session)) -> Dict[str, List]:
    """
    Devuelve un JSON con dos listas:
    - 'concentrix_schedules': registros de Schedule con date == hoy.
    - 'ubycall_schedules':    registros de UbycallSchedule con date == hoy.
    """
    today = date.today()

    conc = session.exec(
        select(Schedule).where(Schedule.date == today)
    ).all()
    uby = session.exec(
        select(UbycallSchedule).where(UbycallSchedule.date == today)
    ).all()

    return {
        "concentrix_schedules": conc,
        "ubycall_schedules":    uby
    }
