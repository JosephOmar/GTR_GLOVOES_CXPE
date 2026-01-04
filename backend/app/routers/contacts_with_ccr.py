from typing import List
from fastapi import APIRouter, UploadFile, Depends, File, HTTPException
from sqlmodel import Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import text

from app.database.database import get_session
from app.services.contacts_with_ccr import contacts_with_ccr_service
from app.schemas.contacts_with_ccr import ContactsReceivedRead
from app.crud.contacts_with_ccr import get_all_contacts_with_ccr

router = APIRouter(tags=["contacts-with-ccr"])

@router.post(
    "/upload-contacts-with-ccr/",
    summary="Carga y procesa contactos con CCR"
)
async def upload_contacts_with_ccr(
    file: UploadFile = File(...),
    session: Session = Depends(get_session),
):
    try:
        result = await contacts_with_ccr_service(file, session)
        return result
    except Exception as e:
        print(f"Error en upload_contacts_with_ccr: {e}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.get(
    "/contacts-with-ccr/",
    response_model=List[ContactsReceivedRead],
    summary="Obtiene todos los registros de Contacts Received con CCR"
)
def read_contacts_with_ccr(
    session: Session = Depends(get_session),
):
    try:
        result = get_all_contacts_with_ccr(session)
        return result

    except SQLAlchemyError as e:
        print(f"Error de base de datos: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno de la base de datos")

    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")

@router.delete(
    "/truncate-contacts-with-ccr/",
    summary="Elimina todos los registros de contactsreceived y contactsreceivedreason"
)
def truncate_contacts_with_ccr(
    session: Session = Depends(get_session),
):
    try:
        session.exec(text("TRUNCATE contactsreceivedreason;"))
        session.exec(text("TRUNCATE contactsreceived CASCADE;"))
        session.commit()
        return {"detail": "Tablas truncadas correctamente"}
    
    except SQLAlchemyError as e:
        session.rollback()
        print(f"Error de base de datos: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno de la base de datos")
    
    except Exception as e:
        session.rollback()
        print(f"Error inesperado: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno del servidor")