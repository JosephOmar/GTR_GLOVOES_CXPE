from typing import List
from fastapi import APIRouter, UploadFile, Depends, File
from sqlmodel import Session

from app.database.database import get_session
from app.services.real_time_data_service import real_time_data_service
from app.schemas.real_time_data import RealTimeDataRead
from app.crud.real_time_data import get_all_real_time_data
from fastapi import HTTPException
from typing import List
from sqlalchemy.exc import SQLAlchemyError  # Importar para manejar errores de SQLAlchemy
from sqlmodel import Session

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
def read_real_time_data(
    session: Session = Depends(get_session)
):
    try:
        data = get_all_real_time_data(session)
        
        # Manejar valores None en el campo 'tht'
        for record in data:
            if record.tht is None:
                record.tht = 0.0  # Asignar un valor predeterminado como 0.0

        return data
    except SQLAlchemyError as e:
        print(f"Error de base de datos: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno de la base de datos")
    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")
