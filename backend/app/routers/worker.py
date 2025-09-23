from fastapi import APIRouter, UploadFile, Depends, File
from sqlmodel import Session, select
from sqlalchemy.orm import selectinload
from typing import List

from app.database.database import get_session
from app.services.workers_service import process_and_persist_workers
from app.models.worker import Worker
from app.schemas.worker import WorkerRead
from datetime import date, timedelta
from app.routers.protected import get_current_user
from app.models.user import User

router = APIRouter()

@router.post("/upload-workers/")
async def upload_workers(
    files: List[UploadFile] = File(...),
    session: Session = Depends(get_session)
):
    count = await process_and_persist_workers(
        files,
        session
    )
    return {"message": f"Se insertaron {count} trabajadores correctamente."}

@router.get(
    "/workers/",
    response_model=List[WorkerRead],
    summary="Lista todos los trabajadores con sus horarios"
)
def read_workers(session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    # Cargamos role, status, campaign, etc. y además schedules y ubycall_schedules
    #current_day = date.today()
    statement = (
        select(Worker)
        .options(
            selectinload(Worker.role),
            selectinload(Worker.status),
            selectinload(Worker.campaign),
            selectinload(Worker.team),
            selectinload(Worker.work_type),
            selectinload(Worker.contract_type),
            selectinload(Worker.schedules),
            selectinload(Worker.ubycall_schedules),
        )
    )
    workers = session.exec(statement).all()

    # for w in workers:
    #     w.schedules = [s for s in w.schedules if s.date == current_day]
    #     w.ubycall_schedules = [u for u in w.ubycall_schedules if u.date == current_day]

    return workers  