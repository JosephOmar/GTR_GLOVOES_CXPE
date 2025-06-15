# app/api/routers/real_data_view.py
from typing import List
from fastapi import APIRouter, UploadFile, Depends, File
from sqlmodel import Session

from app.database.database import get_session
from app.services.operational_view_service import process_and_persist_operational_view
from app.crud.operational_view import get_all_views
from app.models.operational_view import OperationalView

router = APIRouter(tags=["operational-view"])

@router.post("/upload-operational-view/", summary="Carga y procesa datos reales")
async def upload_real_data_view(
    files: List[UploadFile] = File(...),
    session: Session = Depends(get_session),
):
    """
    Recibe 10 archivos, los procesa y persiste en BD.
    Devuelve un resumen de cuántos registros se insertaron/actualizaron.
    """
    result = await process_and_persist_operational_view(files, session)
    return result

@router.get(
    "/operational-view/",
    response_model=List[OperationalView],
    summary="Obtiene todos los registros de OperationalView"
)
def read_operational_view(
    session: Session = Depends(get_session)
):
    return get_all_views(session)
