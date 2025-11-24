from typing import List
from fastapi import APIRouter, UploadFile, Depends, File
from sqlmodel import Session

from app.database.database import get_session
from app.services.sla_breached_service import sla_breached_service
from app.schemas.sla_breached import SlaBreachedRead
from app.crud.sla_breached import get_all_sla_breached_data
from fastapi import HTTPException
from sqlalchemy.exc import SQLAlchemyError  # Importar para manejar errores de SQLAlchemy

router = APIRouter(tags=["sla-breached-data"])

@router.post("/upload-sla-breached-data/", summary="Carga y procesa datos de SLA Breached")
async def upload_sla_breached_data(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
):
    """
    Recibe el archivo con datos de SLA Breached,
    lo procesa, limpia y persiste en la BD.
    Devuelve un resumen de cuántos registros se insertaron/actualizaron.
    """
    try:
        result = await sla_breached_service(file, session)
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al procesar el archivo: {str(e)}")

@router.get(
    "/sla-breached-data/",
    response_model=List[SlaBreachedRead],
    summary="Obtiene todos los registros de SLA Breached"
)
def read_sla_breached_data(
    session: Session = Depends(get_session)
):
    try:
        data = get_all_sla_breached_data(session)
        
        # Manejar valores None en el campo 'chat_breached'
        for record in data:
            if record.chat_breached is None:
                record.chat_breached = 0  # Asignar un valor predeterminado como 0
        print('xd')
        return data
    except SQLAlchemyError as e:
        print(f"Error de base de datos: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno de la base de datos")
    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
