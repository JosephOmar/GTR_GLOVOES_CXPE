import pandas as pd
import numpy as np
from app.core.utils.real_data_view.columns_names import TEAM, DATE, TIME_INTERVAL, SERVICE_LEVEL, REAL_RECEIVED

COLUMNS_ASSEMBLED_DATA = {
    'Start Time': TIME_INTERVAL,
    'Queue': TEAM,
    'Service Level - Actual': SERVICE_LEVEL,
    'Contacts Received - Actual': REAL_RECEIVED,
}

def clean_assembled_data(data_chat: pd.DataFrame, data_call: pd.DataFrame) -> pd.DataFrame:
    # 1) Concatenar
    data = pd.concat([data_chat, data_call], ignore_index=True)

    # 2) Renombrar columnas
    data = data.rename(columns=COLUMNS_ASSEMBLED_DATA)

    # 3) Extraer fecha y hora de la columna TIME_INTERVAL
    #    — Primero convertimos a datetime completo (fecha + hora)
    data['__DATETIME__'] = pd.to_datetime(data[TIME_INTERVAL], errors='coerce')

    #    — Nueva columna DATE con sólo la fecha (tipo datetime.date)
    data[DATE] = data['__DATETIME__'].dt.date

    #    — Reformatear TIME_INTERVAL a '%H:%M'
    data[TIME_INTERVAL] = data['__DATETIME__'].dt.strftime('%H:%M')

    # 4) Ya no necesitamos el auxiliar
    data = data.drop(columns='__DATETIME__')

    # 5) Convertir servicio y contactos
    data[SERVICE_LEVEL] = data[SERVICE_LEVEL].str.rstrip('%').astype(float)

    # 6) Merge de colas “customer”
    merge_queues = ['Spain Customer Verify ID', 'Spain Customers']
    data_customer = data[data[TEAM].isin(merge_queues)].copy()
    data_others = data[~data[TEAM].isin(merge_queues)].copy()

    data_customer = (
        data_customer
        .groupby([DATE, TIME_INTERVAL])
        .apply(lambda grp: pd.Series({
            REAL_RECEIVED: grp[REAL_RECEIVED].sum(),
            SERVICE_LEVEL: (
                (grp[SERVICE_LEVEL] * grp[REAL_RECEIVED]
                 ).sum() / grp[REAL_RECEIVED].sum()
                if grp[REAL_RECEIVED].sum() != 0 else np.nan
            )
        }))
        .reset_index()
    )
    data_customer[TEAM] = 'CHAT CUSTOMER'

    # 7) Renombrar otros equipos
    data_others[TEAM] = data_others[TEAM].replace({
        'Spain Glovers': 'CHAT RIDER',
        'Spain Partners': 'CALL VENDORS'
    })

    # 8) Concatenar resultado final
    data_final = pd.concat([data_customer, data_others], ignore_index=True)

    # 9) Redondeo y reordenar columnas
    data_final[SERVICE_LEVEL] = data_final[SERVICE_LEVEL].round(2)
    data_final = data_final[[DATE, TIME_INTERVAL,
                             TEAM, REAL_RECEIVED, SERVICE_LEVEL]]

    return data_final