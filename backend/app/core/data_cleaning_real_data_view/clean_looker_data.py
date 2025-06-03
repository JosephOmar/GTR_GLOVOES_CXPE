import pandas as pd
from app.core.utils.real_data_view.columns_names import TEAM, TIME_INTERVAL, SAT_ABUSER, AHT

COLUMNS_LOOKER_DATA = {
    'Period (Local Time)' : TIME_INTERVAL,
    'SAT (with abusers)' : SAT_ABUSER,
    'AHT (seconds)' : AHT,
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

    data = data[[TIME_INTERVAL, TEAM, SAT_ABUSER, AHT]]

    data[TIME_INTERVAL] = pd.to_datetime(data[TIME_INTERVAL], errors='coerce')

    data[TIME_INTERVAL] = data[TIME_INTERVAL].dt.strftime('%H:%M')

    return data