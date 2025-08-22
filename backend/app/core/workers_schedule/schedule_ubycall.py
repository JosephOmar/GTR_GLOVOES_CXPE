# app/core/workers_schedule/schedule_ubycall.py

import pandas as pd
from datetime import time
from app.core.utils.workers_cx.columns_names import DOCUMENT, DATE, START_TIME, END_TIME, DAY

SCHEDULE_UBYCALL_COLUMNS = {
    'DNI':        DOCUMENT,
    'FECHA':      DATE,
    'HORAINICIO': START_TIME,
    'HORAFIN':    END_TIME
}

def schedule_ubycall(data: pd.DataFrame) -> pd.DataFrame:
    data = data.rename(columns=SCHEDULE_UBYCALL_COLUMNS)

    data[DATE] = pd.to_datetime(data[DATE]).dt.date
    day_map = {
        0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves',
        4: 'Viernes', 5: 'Sábado', 6: 'Domingo'
    }
    data[DAY] = pd.to_datetime(data[DATE]).dt.weekday.map(day_map)

    # vectorizado + truncado al minuto
    for col in (START_TIME, END_TIME):
        data[col] = (
            pd.to_datetime(data[col], format='%H:%M:%S', errors='coerce')
              .dt.floor('min')
              .dt.time
        )
        # asegurar sólo datetime.time o None
        data[col] = data[col].apply(lambda x: x if isinstance(x, time) else None)

    data[DOCUMENT] = data[DOCUMENT].astype(str).str.strip()
    print(data[[DOCUMENT, DATE, DAY, START_TIME, END_TIME]])
    return data[[DOCUMENT, DATE, DAY, START_TIME, END_TIME]]
