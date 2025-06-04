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
    # 1) Asignar equipo a los parciales
    data_looker_CR[TEAM] = 'CHAT CUSTOMER'
    data_looker_RD[TEAM] = 'CHAT RIDER'

    # 2) Concatenar y renombrar columnas
    data = pd.concat([data_looker_CR, data_looker_RD], ignore_index=True)
    data = data.rename(columns=COLUMNS_LOOKER_DATA)
    data = data[[REAL_RECEIVED, SERVICE_LEVEL, TIME_INTERVAL, TEAM, SAT_ABUSER, AHT]]

    # 3) Formatear TIME_INTERVAL a 'HH:MM'
    data[TIME_INTERVAL] = (
        pd.to_datetime(data[TIME_INTERVAL], errors='coerce')
          .dt.strftime('%H:%M')
    )

    # 4) Convertir SERVICE_LEVEL de 'XX%' a float
    data[SERVICE_LEVEL] = (
        data[SERVICE_LEVEL]
          .str.rstrip('%')
          .astype(float)
          .round(2)
    )

    # 5) Generar todos los intervalos de 30 minutos entre el mínimo y máximo
    time_intervals = (
        pd.date_range(
            start=data[TIME_INTERVAL].min(),
            end=data[TIME_INTERVAL].max(),
            freq='30T'
        )
        .strftime('%H:%M')
    )
    time_intervals_df = pd.DataFrame({TIME_INTERVAL: time_intervals})

    all_data = pd.DataFrame()

    for team_name in ['CHAT CUSTOMER', 'CHAT RIDER']:
        team_data = data[data[TEAM] == team_name]

        # 6) Merge para asegurar todos los intervalos
        team_data_complete = pd.merge(
            time_intervals_df,
            team_data,
            on=TIME_INTERVAL,
            how='left'
        )

        # 7) Rellenar REAL_RECEIVED con 0, el resto (incluyendo AHT) queda como NaN
        team_data_complete = team_data_complete.fillna({REAL_RECEIVED: 0})
        team_data_complete[TEAM] = team_name

        all_data = pd.concat([all_data, team_data_complete], ignore_index=True)

    # 8) Limpiar comas y espacios en AHT para luego convertir a numérico
    #    - Primero nos aseguramos de que sean cadenas (para poder reemplazar comas)
    all_data[AHT] = all_data[AHT].astype(str).str.replace(' ', '', regex=False)
    all_data[AHT] = all_data[AHT].str.replace(',', '', regex=False)

    # 9) Convertir a numérico, forzando a NaN donde no se pueda parsear
    all_data[AHT] = pd.to_numeric(all_data[AHT], errors='coerce')

    # 10) Si quieres forzar que AHT sea entero pero permitiendo NaN,
    #     puedes usar el dtype nullable Int64 de pandas:
    all_data[AHT] = all_data[AHT].astype('Int64')

    return all_data
