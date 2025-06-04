import pandas as pd
from app.core.utils.real_data_view.columns_names import (
    TEAM, TIME_INTERVAL, SAT_ABUSER, AHT, REAL_RECEIVED, SERVICE_LEVEL
)

COLUMNS_LOOKER_DATA = {
    'Total contacts received': REAL_RECEIVED,
    '% SLA FRT': SERVICE_LEVEL,
    'Period (Local Time)': TIME_INTERVAL,
    'SAT (with abusers)': SAT_ABUSER,
    'AHT (seconds)': AHT,
}

def clean_looker_data(
    data_looker_CR: pd.DataFrame,
    data_looker_RD: pd.DataFrame,
) -> pd.DataFrame:
    # Asignar equipo a los parciales
    data_looker_CR[TEAM] = 'CHAT CUSTOMER'
    data_looker_RD[TEAM] = 'CHAT RIDER'

    data = pd.concat([data_looker_CR, data_looker_RD], ignore_index=True)
    data = data.rename(columns=COLUMNS_LOOKER_DATA)
    data = data[[REAL_RECEIVED, SERVICE_LEVEL, TIME_INTERVAL, TEAM, SAT_ABUSER, AHT]]

    # Convertir el texto de TIME_INTERVAL a datetime y luego a 'HH:MM'
    data[TIME_INTERVAL] = pd.to_datetime(data[TIME_INTERVAL], errors='coerce').dt.strftime('%H:%M')

    # Convertir SERVICE_LEVEL de string con '%' a float
    data[SERVICE_LEVEL] = data[SERVICE_LEVEL].str.rstrip('%').astype(float).round(2)

    # Crear la serie de intervalos cada 30 min
    time_intervals = pd.date_range(
        start=data[TIME_INTERVAL].min(),
        end=data[TIME_INTERVAL].max(),
        freq='30T'
    ).strftime('%H:%M')

    time_intervals_df = pd.DataFrame({TIME_INTERVAL: time_intervals})

    all_data = pd.DataFrame()

    for team_name in ['CHAT CUSTOMER', 'CHAT RIDER']:
        team_data = data[data[TEAM] == team_name]

        # Merge para que aparezcan todos los intervalos
        team_data_complete = pd.merge(
            time_intervals_df,
            team_data,
            on=TIME_INTERVAL,
            how='left'
        )

        # Solo rellenar REAL_RECEIVED con 0, dejar SERVICE_LEVEL, SAT_ABUSER y AHT como NaN
        team_data_complete = team_data_complete.fillna({REAL_RECEIVED: 0})

        # Asignar el equipo en todas las filas (antes quedaban NaN para las filas creadas por el merge)
        team_data_complete[TEAM] = team_name

        all_data = pd.concat([all_data, team_data_complete], ignore_index=True)

    print(all_data[all_data[TEAM] == 'CHAT RIDER'].head(48))
    
    return all_data
