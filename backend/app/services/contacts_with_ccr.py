from fastapi import UploadFile, HTTPException
from app.services.utils.upload_service import handle_file_upload_generic
from app.core.contacts_with_ccr.clean_contacts_with_ccr import clean_contacts_with_ccr
from app.models.real_time_data import RealTimeData
from datetime import date, datetime
from sqlmodel import Session, delete, select
import pandas as pd
import traceback  # 👈 importante para imprimir errores completos
from app.utils.validators.validate_excel_contacts_with_ccr import validate_excel_contacts_with_ccr, CONTACTS_MAPPING
from app.crud.contacts_with_ccr import upsert_contacts_with_ccr

async def contacts_with_ccr_service(file1: UploadFile, session: Session):
    try:
        df_received, df_reason = await handle_file_upload_generic(
            files=[file1],
            validator=validate_excel_contacts_with_ccr,
            keyword_to_slot=CONTACTS_MAPPING,
            required_slots=list(CONTACTS_MAPPING.values()),
            post_process=clean_contacts_with_ccr
        )

        df_received = df_received.where(pd.notnull(df_received), None)
        df_reason = df_reason.where(pd.notnull(df_reason), None)

        received_data = df_received.to_dict(orient="records")
        reason_data = df_reason.to_dict(orient="records")

        result = upsert_contacts_with_ccr(
            session=session,
            received_data=received_data,
            reason_data=reason_data
        )

        return result


    except Exception as e:
        print(f"❌ Error inesperado en contacts_with_ccr_service: {e}")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
