from fastapi import UploadFile, HTTPException
from app.services.utils.upload_service import handle_file_upload_generic
from app.core.real_time_data.clean_real_time_data import clean_real_time_data
from app.models.real_time_data import RealTimeData
from datetime import date, datetime
from sqlmodel import Session, delete, select
import pandas as pd
import traceback  # 👈 importante para imprimir errores completos
from app.utils.validators.validate_excel_real_time_data import validate_excel_real_time_data
_KPI_SLOTS = {"real_time_data": "real_time_data"}
_REQUIRED_KPI = ["real_time_data"]

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


async def real_time_data_service(file1: UploadFile, session: Session):
    print("🟢 Iniciando procesamiento del archivo:", file1.filename)

    try:
        df = await handle_file_upload_generic(
            files=[file1],
            validator=validate_excel_real_time_data,
            keyword_to_slot=_KPI_SLOTS,
            required_slots=_REQUIRED_KPI,
            post_process=lambda real_time_data, **kw: clean_real_time_data(real_time_data),
        )
        print("✅ Archivo procesado correctamente. Columnas:", df.columns.tolist())
        print("✅ Filas:", len(df))
    except Exception as e:
        print("❌ Error al procesar el archivo:")
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=f"Error en lectura o limpieza: {str(e)}")

    # Consultamos los datos existentes en la base de datos
    statement = select(RealTimeData)
    results = session.exec(statement)
    existing_data = {
        (record.team, record.date, record.interval): record for record in results
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
            contacts_received = safe_int(row.get("contacts_received"))
            sla_frt = safe_float(row.get("SLA FRT"))
            tht = safe_float(row.get("THT"))

            # Comprobamos si el registro ya existe
            if (team, date, interval) in existing_data:
                # Si el registro existe, comparamos el contacts_received
                existing_record = existing_data[(team, date, interval)]
                if contacts_received > existing_record.contacts_received:
                    # Si el nuevo valor es mayor, actualizamos el registro
                    existing_record.contacts_received = contacts_received
                    existing_record.sla_frt = sla_frt
                    existing_record.tht = tht
                    session.add(existing_record)
                    rows_updated += 1
            else:
                # Si el registro no existe, lo insertamos
                new_record = RealTimeData(
                    team=team,
                    date=date,
                    interval=interval,
                    contacts_received=contacts_received,
                    sla_frt=sla_frt,
                    tht=tht
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

