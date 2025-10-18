import pandas as pd
import numpy as np
from app.core.utils.planned.columns_names import TEAM, DATE, TIME_INTERVAL, SERVICE_LEVEL, REAL_RECEIVED, AHT

COLUMNS_ASSEMBLED_DATA = {
    'Start Time': TIME_INTERVAL,
    'Queue': TEAM,
    'Service Level - Actual': SERVICE_LEVEL,
    'Contacts Received - Actual': REAL_RECEIVED,
}

def clean_assembled_data(data_call: pd.DataFrame, data_TD: pd.DataFrame ) -> pd.DataFrame:

    # 2) Renombrar columnas
    data_call = data_call.rename(columns=COLUMNS_ASSEMBLED_DATA)

    # 3) Extraer fecha y hora de la columna TIME_INTERVAL
    #    — Primero convertimos a datetime completo (fecha + hora)
    data_call['__DATETIME__'] = pd.to_datetime(data_call[TIME_INTERVAL], errors='coerce')

    #    — Nueva columna DATE con sólo la fecha (tipo datetime.date)
    data_call[DATE] = data_call['__DATETIME__'].dt.date

    #    — Reformatear TIME_INTERVAL a '%H:%M'
    data_call[TIME_INTERVAL] = data_call['__DATETIME__'].dt.strftime('%H:%M')

    # 4) Ya no necesitamos el auxiliar
    data_call = data_call.drop(columns='__DATETIME__')

    # 5) Convertir servicio y contactos
    data_call[SERVICE_LEVEL] = data_call[SERVICE_LEVEL].str.rstrip('%').astype(float)

    # 7) Renombrar otros equipos
    data_call[TEAM] = data_call[TEAM].replace({
        'Spain Partners': 'CALL VENDORS'
    })

    # 9) Redondeo y reordenar columnas
    data_call[SERVICE_LEVEL] = data_call[SERVICE_LEVEL].round(2)

    data_call = data_call[[DATE, TIME_INTERVAL, TEAM, REAL_RECEIVED, SERVICE_LEVEL]]

    data_TD = data_TD[data_TD['Call Type'] == 'inbound']

    # Convertir las columnas necesarias
    data_TD['End Time'] = pd.to_datetime(data_TD['End Time'])
    data_TD['Talk Time'] = pd.to_timedelta(data_TD['Talk Time'])

    # Crear los rangos de 30 minutos
    data_TD['Time Block'] = data_TD['End Time'].dt.floor('30min')

    # Convertir AHT a segundos
    data_TD['Talk Time (seconds)'] = data_TD['Talk Time'].dt.total_seconds()


    # Calcular el AHT por cada rango de 30 minutos en segundos
    data_TD = data_TD.groupby('Time Block')['Talk Time (seconds)'].mean().reset_index()

    # Formatear Time Block para que solo muestre la hora y minutos
    data_TD['Time Block'] = data_TD['Time Block'].dt.strftime('%H:%M')

    data_TD = data_TD.rename(columns={'Time Block': TIME_INTERVAL, 'Talk Time (seconds)': AHT})
    data_TD[AHT] = data_TD[AHT].round(0)
    data_final = pd.merge(data_call, data_TD, on=[TIME_INTERVAL], how='outer')
   
    return data_final