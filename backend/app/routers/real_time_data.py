from typing import List
from fastapi import APIRouter, UploadFile, Depends, File
from sqlmodel import Session

from app.database.database import get_session
from app.services.real_time_data_service import real_time_data_service
from app.schemas.real_time_data import RealTimeDataRead
from app.crud.real_time_data import get_all_real_time_data

router = APIRouter(tags=["real-time-data"])

@router.post("/upload-real-time-data/", summary="Carga y procesa datos planificados")
async def upload_real_time_data(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
):
    """
    Recibe el archivo con datos planificados (real_time_data),
    lo procesa, limpia y persiste en la BD.
    Devuelve un resumen de cuántos registros se insertaron/actualizaron.
    """
    print('si llega aquí')
    result = await real_time_data_service(file, session)
    return result

@router.get(
    "/real-time-data/",
    response_model=List[RealTimeDataRead],
    summary="Obtiene todos los registros de Planned"
)
def read_planned_data(
    session: Session = Depends(get_session)
):
    return get_all_real_time_data(session)
