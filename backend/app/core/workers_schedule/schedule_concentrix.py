# app/core/workers_schedule/schedule_concentrix.py

import pandas as pd
from datetime import datetime, date, timedelta, time
from app.core.utils.workers_cx.columns_names import (
    DOCUMENT, DATE, START_TIME, END_TIME,
    DAY, BREAK_START, BREAK_END, REST_DAY
)


def schedule_concentrix(
    data: pd.DataFrame,
    data_obs: pd.DataFrame,
    week: int | None = None,
    year: int | None = None
) -> pd.DataFrame:
    # Si no reciben año o semana, tomamos la actual
    data['NRO_DOCUMENTO'] = data['NRO_DOCUMENTO'].astype(str).str.lstrip("0")
    data_obs['NRO_DOCUMENTO'] = data_obs['NRO_DOCUMENTO'].astype(
        str).str.lstrip("0")
    today = date.today()
    year = year or today.year
    week = week or today.isocalendar()[1]
    # date.fromisocalendar(year, week, 1) devuelve el lunes de esa semana ISO
    monday = date.fromisocalendar(year, week, 1)

    days = ['Lunes', 'Martes', 'Miercoles',
            'Jueves', 'Viernes', 'Sabado', 'Domingo']
    idx_map = {d: i for i, d in enumerate(days)}

    records = []
    for _, row in data.iterrows():
        doc = str(row['NRO_DOCUMENTO']).strip()
        for day in days:
            idx = idx_map[day]
            current_date = monday + timedelta(days=idx)

            ing = row.get(f'INGRESO_{day.upper()}')
            sal = row.get(f'SALIDA_{day.upper()}')
            ref = row.get(f'REFRIGERIO_{day.upper()}')

            # Verificar si el día corresponde a un descanso
            if pd.isna(ing) or str(ing).strip().upper() == 'DSO':
                rest = True
                rec = {START_TIME: None, END_TIME: None,
                       BREAK_START: None, BREAK_END: None}
            else:
                rest = False
                rec = {
                    START_TIME: ing,
                    END_TIME: sal,
                    BREAK_START: (ref.split('-')[0].strip() if isinstance(ref, str) and '-' in ref else None),
                    BREAK_END: (
                        ref.split('-')[1].strip() if isinstance(ref, str) and '-' in ref else None)
                }

            # Función para verificar si el valor es un horario (hora)
            def is_time(value):
                try:
                    # Intentamos convertir a hora
                    pd.to_datetime(value, format='%H:%M:%S', errors='raise')
                    return True
                except (ValueError, TypeError):
                    # Si no se puede convertir, no es una hora
                    return False

            # Modificación en la sección de agregar la columna 'obs'
            obs = None
            vac_record = data_obs[(data_obs['NRO_DOCUMENTO'] == str(doc)) &
                                  (data_obs[current_date.strftime('%d/%m/%Y')].notna())]

            if not vac_record.empty:
                # Obtener el valor de la columna correspondiente
                obs_value = vac_record[current_date.strftime(
                    '%d/%m/%Y')].values[0]

                # Si el valor no es un horario (hora), asignamos el valor a 'obs'
                if not is_time(obs_value):
                    obs = obs_value  # Lo dejamos tal cual si no es hora
                else:
                    obs = None  # Si es hora, lo omitimos

            # Crear el registro con la columna 'obs'
            records.append({
                DOCUMENT: doc,
                DATE: current_date,
                DAY: day,
                REST_DAY: rest,
                **rec,
                'obs': obs  # Añadimos la columna 'obs'
            })

    # Crear el DataFrame final
    df = pd.DataFrame(records)

    # Convertir & truncar & asegurar time
    for col in (START_TIME, END_TIME, BREAK_START, BREAK_END):
        df[col] = (
            pd.to_datetime(df[col], errors='coerce')
            .dt.floor('min')
            .dt.time
        )
        df[col] = df[col].apply(lambda x: x if isinstance(x, time) else None)

    # Devolvemos el DataFrame con la nueva columna 'obs'
    return df[[DOCUMENT, DATE, DAY, START_TIME, END_TIME, BREAK_START, BREAK_END, REST_DAY, 'obs']]
