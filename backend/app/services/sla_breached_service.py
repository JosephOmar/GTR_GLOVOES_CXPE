from fastapi import UploadFile, HTTPException
from app.services.utils.upload_service import handle_file_upload_generic
from app.core.sla_breached.clean_sla_breached import clean_sla_breached
from app.models.sla_breached import SlaBreached
from sqlmodel import Session, select
import pandas as pd
import traceback
from datetime import date, datetime
from app.utils.validators.validate_excel_sla_breached import validate_excel_sla_breached

_KPI_SLOTS = {"sla_breached": "sla_breached"}
_REQUIRED_KPI = ["sla_breached"]

def safe_str(v): return str(v).strip() if v is not None else None

def safe_date(v):
    if v is None or pd.isna(v):
        return None
    if isinstance(v, datetime):
        return v.date()
    try:
        return pd.to_datetime(v).date()
    except Exception:
        return None

def safe_float(v):
    try:
        return float(v) if v is not None else None
    except Exception:
        return None

def safe_int(v):
    try:
        return int(v) if v is not None else None
    except Exception:
        return None


async def sla_breached_service(file1: UploadFile, session: Session):
    print("🟢 Iniciando procesamiento del archivo:", file1.filename)

    try:
        # Cargar y validar archivo usando el servicio de carga genérico
        df = await handle_file_upload_generic(
            files=[file1],
            validator=validate_excel_sla_breached,  # Puedes agregar una función de validación si es necesario
            keyword_to_slot=_KPI_SLOTS,
            required_slots=_REQUIRED_KPI,
            post_process=lambda sla_breached, **kw: clean_sla_breached(sla_breached),  # Llamada a la función de limpieza
        )
        print("✅ Archivo procesado correctamente. Columnas:", df.columns.tolist())
        print("✅ Filas:", len(df))
    except Exception as e:
        print("❌ Error al procesar el archivo:")
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"Error en lectura o limpieza: {str(e)}")

    # Consultamos los datos existentes en la base de datos
    statement = select(SlaBreached)
    results = session.exec(statement)
    existing_data = {
        (record.team, record.date, record.interval, record.api_email): record for record in results
    }
    print("✅ Datos existentes cargados de la base de datos.")

    rows_inserted = 0
    rows_updated = 0

    try:
        # Iteramos sobre las filas del archivo procesado (df)
        print("💾 Insertando o actualizando registros...")

        for _, row in df.iterrows():
            team = safe_str(row.get("team"))
            date = safe_date(row.get("date"))
            interval = safe_str(row.get("interval"))
            api_email = safe_str(row.get("api_email"))
            chat_breached = safe_int(row.get("chat_breached"))

            # Comprobamos si el registro ya existe
            if (team, date, interval, api_email) in existing_data:
                # Si el registro existe, comparamos el chat_breached
                existing_record = existing_data[(team, date, interval, api_email)]
                if chat_breached > existing_record.chat_breached:
                    # Si el nuevo valor es mayor, actualizamos el registro
                    existing_record.chat_breached = chat_breached
                    session.add(existing_record)
                    rows_updated += 1
            else:
                # Si el registro no existe, lo insertamos
                new_record = SlaBreached(
                    team=team,
                    date=date,
                    interval=interval,
                    api_email=api_email,
                    chat_breached=chat_breached
                )
                session.add(new_record)
                rows_inserted += 1

        session.commit()
        print(f"✅ Insertadas {rows_inserted} filas y actualizadas {rows_updated} filas correctamente.")
        return {"status": "success", "rows_inserted": rows_inserted, "rows_updated": rows_updated}

    except Exception as e:
        print("❌ Error durante la inserción o actualización:")
        traceback.print_exc()
        session.rollback()
        raise HTTPException(status_code=500, detail=f"Error insertando o actualizando registros: {str(e)}")
