import pandas as pd
from fastapi import HTTPException, UploadFile
from datetime import date, timedelta, datetime, time
from sqlmodel import Session, select, delete, and_
import traceback

from app.services.utils.upload_service import handle_file_upload_generic
from app.utils.validators.validate_excel_attendance import validate_excel_attendance
from app.core.workers_attendance.attendance import clean_attendance
from app.models.worker import Worker, Schedule, Attendance


async def process_and_persist_attendance(
    file: UploadFile,
    session: Session,
    target_date: date | None = None,
) -> dict:
    """
    Procesa archivo de asistencia, limpia los datos, calcula el estado (present, late, absent),
    determina check_in, check_out, el tiempo fuera de adherencia (out_of_adherence)
    y el tiempo total desconectado dentro del turno (offline_minutes).
    """
    try: 
        try:
            # 1️⃣ Leer Excel validado
            df_raw, = await handle_file_upload_generic(
                files=[file],
                validator=validate_excel_attendance,
                keyword_to_slot={"attendance": "attendance"},
                required_slots=["attendance"],
                post_process=lambda att: (att, )
            )
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=400, detail=f"Error al leer archivo: {e}")
        
        print(target_date)
        # 2️⃣ Normalizar data
        try:
            df_attendance = clean_attendance(df_raw, target_date)
            if df_attendance.empty:
                raise HTTPException(status_code=400, detail="No se encontraron registros de asistencia")
        except Exception as e:
            traceback.print_exc()
            raise HTTPException(status_code=400, detail=f"Error al procesar la data: {e}")

        # 3️⃣ Si no se pasa fecha, tomamos la de la data (primer registro)
        if not target_date:
            target_date = df_attendance["date"].iloc[0]

        # 4️⃣ Purga asistencias de ese mismo día (evita duplicados por nueva carga)
        print(f"Eliminando registros para la fecha: {target_date}")
        session.exec(
            delete(Attendance).where(Attendance.date == target_date)  # Comparar solo la parte de la fecha
        )
        session.commit()

        inserted = 0
        missing_workers = []

        for row in df_attendance.itertuples(index=False):
            api_email = str(row.api_email).strip()
            date_row = row.date
            check_in_times = row.check_in_times
            check_out_times = row.check_out_times

            # Verificar si el trabajador existe
            worker = session.exec(
                select(Worker).where(Worker.api_email == api_email)
            ).first()

            if not worker:
                # Si no encontramos el trabajador, podemos optar por no insertarlo
                missing_workers.append(api_email)
                continue  # O bien, podrías agregar al trabajador si es necesario

            schedule = session.exec(
                select(Schedule).where(
                    and_(
                        Schedule.worker_document == worker.document,
                        Schedule.date == date_row
                    )
                )
            ).first()

            if not schedule:
                continue  # Si no hay horario para el trabajador, lo saltamos.

            if schedule.start_time is None or schedule.end_time is None:
                continue

            # Convertir start_time y end_time a datetime para realizar operaciones con timedelta
            start_datetime = datetime.combine(date_row, schedule.start_time)  # Combinar con la fecha
            end_datetime = datetime.combine(date_row, schedule.end_time)      # Combinar con la fecha

            # Variables para los cálculos
            total_out_of_adherence = 0
            total_offline_minutes = 0

            # Filtrar solo el primer check_in que sea válido
            valid_check_in_times = [
                check_in for check_in in check_in_times
                if datetime.combine(date_row, check_in[0]) >= start_datetime - timedelta(hours=3)
            ]
            valid_check_in_times.sort()  # Ordenar los check_in por hora (el primero será el más temprano)

            check_in = None
            check_out = None
            status = "Absent" 

            if valid_check_in_times:
                # Tomar el primer check_in válido para determinar el estado
                check_in = valid_check_in_times[0]
                check_in_time = datetime.combine(date_row, check_in[0])

                # Determinar estado según el primer check_in
                if check_in_time <= start_datetime + timedelta(minutes=10):
                    status = "Present"
                elif check_in_time > start_datetime + timedelta(minutes=10):
                    status = "Late"

                # Calcular tiempo fuera de adherencia (todos los check_in después del end_time)
                for ci in valid_check_in_times:
                    ci_time = datetime.combine(date_row, ci[0])
                    ci_duration = ci[1]

                    ci_end_time = ci_time + timedelta(minutes=ci_duration)

                    # Solo contar la parte que cae después del fin del turno
                    if ci_end_time > end_datetime:
                        # Recortar a máximo 3h después del fin de turno
                        cutoff_time = min(ci_end_time, end_datetime + timedelta(hours=3))

                        # Si el inicio es antes del fin del turno, solo cuenta la parte después del fin
                        if ci_time < end_datetime:
                            overlap_minutes = round((cutoff_time - end_datetime).total_seconds() / 60)
                        else:
                            overlap_minutes = round((cutoff_time - ci_time).total_seconds() / 60)

                        # Evitar negativos
                        if overlap_minutes > 0:
                            total_out_of_adherence += overlap_minutes

                # Filtrar solo el primer check_out que sea válido
            

            # --- CHECK-OUT ---

            current_time = datetime.now()
            
            valid_check_out_times = [
                check_out for check_out in check_out_times
                if datetime.combine(date_row, check_out[0]) > start_datetime
                and datetime.combine(date_row, check_out[0])  <= end_datetime + timedelta(hours=3)
            ]
            
            valid_check_out_times.sort()

            if valid_check_out_times:
                # Buscar un check_out dentro de los últimos 5 minutos antes de la hora final
                window_start = end_datetime - timedelta(minutes=5)
                near_end_check_outs = [
                    co for co in valid_check_out_times
                    if window_start <= datetime.combine(date_row, co[0]) <= end_datetime
                ]

                if near_end_check_outs:
                    # ✅ Caso 1: Hay un check_out cercano al fin del turno (±5 min)
                    check_out = near_end_check_outs[0]
                else:
                    # ✅ Caso 2: No hay check_out cercano, pero buscamos si pasó el límite de 2h
                    last_valid_check_out = valid_check_out_times[-1]
                    last_time = datetime.combine(date_row, last_valid_check_out[0])

                    if last_time > end_datetime + timedelta(hours=2):
                        # ❌ No tomarlo (más de 2h tarde)
                        check_out = None
                    else:
                        # ✅ Tomar el último check_out válido dentro del rango
                        check_out = last_valid_check_out

            # ✅ Caso 3: si aún no ha terminado el turno, no asignar salida
            if current_time < end_datetime:
                check_out = None

            # --- Cálculo de métricas ---
            total_offline_minutes = 0
            for co in valid_check_out_times:
                co_time = datetime.combine(date_row, co[0])
                co_duration = co[1]
                co_end_time = co_time + timedelta(minutes=co_duration)

                # Solo considerar desconexiones que empiezan antes del fin del turno
                if co_time <= end_datetime:
                    # Recortar si la desconexión se extiende más allá del turno
                    valid_end = min(co_end_time, end_datetime)
                    valid_duration = round((valid_end - co_time).total_seconds() / 60)
                    total_offline_minutes += valid_duration

            # Validación para asegurar que check_in y check_out estén definidos
            if valid_check_in_times:
                check_out_time_value = check_out[0] if check_out else None 
                # Insertar registro de asistencia solo si ambos valores existen
                attendance = Attendance(
                    api_email=api_email,
                    date=date_row,
                    check_in=check_in[0],  # Guardamos solo la hora
                    check_out=check_out_time_value,  # Guardamos solo la hora
                    status=status,
                    out_of_adherence=total_out_of_adherence,
                    offline_minutes=total_offline_minutes,
                )
                session.add(attendance)
                inserted += 1

        session.commit()  # Confirmar los cambios

        return {"inserted": inserted, "missing_workers": missing_workers}
    except Exception as e: 
        print("❌ Error inesperado en process_and_persist_schedules:")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
