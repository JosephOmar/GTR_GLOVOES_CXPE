import pandas as pd
from datetime import datetime, timedelta, time
import numpy as np
from app.core.utils.workers_cx.columns_names import DOCUMENT, DATE, START_DATE, END_DATE, START_TIME, END_TIME, BREAK_START, BREAK_END, REST_DAY

def schedule_ppp(df: pd.DataFrame) -> pd.DataFrame:

    # --- 2. Ubicar la columna de DNI (no está bajo una fecha) ---
    try:
        dni_col = next(col for col in df.columns if col[1] == "DNI")
    except StopIteration:
        raise ValueError("No se encontró una columna con header de nivel 1 == 'DNI'")

    # --- 3. Obtener todas las fechas (nivel 0) que son datetime ---
    date_values = sorted({c[0] for c in df.columns if isinstance(c[0], datetime)})

    records = []

    for date in date_values:
        # Columnas de este día que nos interesan
        wanted = {
            "Hora Inicio": START_TIME,
            "Hora Fin": END_TIME,
            "Inicio Almuerzo": BREAK_START,
            "Fin Almuerzo": BREAK_END,
        }

        sub_cols = [dni_col]
        col_renames = {"DNI": DOCUMENT}

        for original_name, new_name in wanted.items():
            col_key = (date, original_name)
            if col_key in df.columns:
                sub_cols.append(col_key)
                col_renames[original_name] = new_name

        # Si no hay ninguna de las columnas de horario para esa fecha, la saltamos
        if len(sub_cols) == 1:
            continue

        # --- 4. Extraer sub-dataframe para esa fecha ---
        sub = df.loc[:, sub_cols].copy()

        # Aplanar columnas: usamos solo el nivel 1 para renombrar
        sub.columns = [c[1] for c in sub.columns]

        # Renombrar a nombres normalizados
        sub = sub.rename(columns=col_renames)

        # Asegurar que existan todas las columnas de tiempo, aunque no estén en el Excel
        for col in [START_TIME, END_TIME, BREAK_START, BREAK_END]:
            if col not in sub.columns:
                sub[col] = None

        # --- 5. Agregar start_date y end_date ---
        # Inicializar las fechas de inicio y fin como el mismo día
        sub[START_DATE] = date.date()  # solo la fecha, sin hora
        sub[END_DATE] = date.date()  # por defecto el mismo día

        # Verificamos si el fin del turno es al día siguiente
        # Verificamos si el fin del turno es al día siguiente
        for i, row in sub.iterrows():
            try:
                start_time = pd.to_timedelta(row[START_TIME])
                end_time = pd.to_timedelta(row[END_TIME])
            except ValueError:
                continue  # Si hay un error en la conversión, se omite esta iteración

            start_time_in_minutes = start_time.total_seconds() / 60  # Convertimos a minutos
            end_time_in_minutes = end_time.total_seconds() / 60      # Convertimos a minutos

            # Verificamos si el fin de turno es antes que el inicio (cruzó medianoche)
            if end_time_in_minutes < start_time_in_minutes:
                # Asignamos el día siguiente al 'end_date'
                sub.at[i, END_DATE] = sub.at[i, END_DATE] + timedelta(days=1)

        # --- 7. rest_day: True si no hay start_time o si es 'DSO' ---
        sub[REST_DAY] = sub[START_TIME].isna()

        # --- 7. Convertir horas a tipo time ---
        for col in [START_TIME, END_TIME, BREAK_START, BREAK_END]:
            sub[col] = sub[col].apply(lambda x: f"{x.components.hours:02}:{x.components.minutes:02}:{x.components.seconds:02}" if isinstance(x, pd.Timedelta) else x)
        # Guardamos registros de este día
        records.append(sub[[DOCUMENT, START_DATE, END_DATE, START_TIME, END_TIME,
                            BREAK_START, BREAK_END, REST_DAY]])

    final_df = pd.concat(records, ignore_index=True)

    for col in (START_TIME, END_TIME, BREAK_START, BREAK_END):
        final_df[col] = pd.to_datetime(final_df[col], format="%H:%M:%S", errors="coerce").dt.time

    for col in (START_TIME, END_TIME, BREAK_START, BREAK_END):
        final_df[col] = final_df[col].apply(lambda x: x if isinstance(x, time) else None)
    # --- 10. Concatenar todo ---
    if not records:
        raise ValueError("No se encontraron columnas de horario para ninguna fecha")

    return final_df