# app/api/routers/planned_data.py

from typing import List
from fastapi import APIRouter, UploadFile, Depends, File
from sqlmodel import Session

from app.database.database import get_session
from app.services.planned_service import planned_service
from app.models.planned import Planned
from app.crud.planned import get_all_planned
from app.schemas.planned import PlannedRead

router = APIRouter(tags=["planned-data"])

@router.post("/upload-planned-data/", summary="Carga y procesa datos planificados")
async def upload_planned_data(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
):
    """
    Recibe el archivo con datos planificados (planned_data),
    lo procesa, limpia y persiste en la BD.
    Devuelve un resumen de cuántos registros se insertaron/actualizaron.
    """
    result = await planned_service(file, session)
    return result

@router.get(
    "/planned-data/",
    response_model=List[PlannedRead],
    summary="Obtiene todos los registros de Planned"
)
def read_planned_data(
    session: Session = Depends(get_session)
):
    return get_all_planned(session)
