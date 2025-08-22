import pandas as pd
import pytz
from app.core.utils.real_data_view.columns_names import TEAM, DATE, TIME_INTERVAL, AGENTS_ONLINE, AGENTS_TRAINING, AGENTS_AUX

COLUMNS_REAL_AGENTS = {
    'Marca temporal': DATE,
    'Seleccione Canal': TEAM,
    'Agentes Conectados (Online)': AGENTS_ONLINE,
    'Agentes Training (Glovo + Others)': AGENTS_TRAINING,
    'Agentes Auxiliares (Otros auxiliares, menos Unavailable)': AGENTS_AUX
}

def clean_real_agents(data: pd.DataFrame) -> pd.DataFrame:
    peru_tz = pytz.timezone('America/Lima')
    madrid_tz = pytz.timezone('Europe/Madrid')

    # Renombrar las columnas
    data = data.rename(columns=COLUMNS_REAL_AGENTS)

    # Asegúrate de que DATE esté en formato datetime
    data[DATE] = pd.to_datetime(data[DATE], format='%d/%m/%Y %H:%M:%S', errors='coerce')

    # Eliminar zona horaria si está presente antes de convertir
    if data[DATE].dt.tz is not None:
        data[DATE] = data[DATE].dt.tz_localize(None)

    # Localizar la fecha en la zona horaria de Perú
    data[DATE] = data[DATE].dt.tz_localize(peru_tz)

    # Convertir a la zona horaria de Madrid
    data[DATE] = data[DATE].dt.tz_convert(madrid_tz)

    # Eliminar la zona horaria después de la conversión
    data[DATE] = data[DATE].dt.tz_localize(None)

    # Extraer la fecha y redondear a 30 minutos
    data[TIME_INTERVAL] = data[DATE].dt.floor('30min').dt.strftime('%H:%M')
    data[DATE] = pd.to_datetime(data[DATE]).dt.date

    # --- LIMPIEZA DE COLUMNAS NUMÉRICAS ---
    for col in [AGENTS_ONLINE, AGENTS_TRAINING, AGENTS_AUX]:
        data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0)

    # Sumar las columnas de agentes
    data['Total_Agentes'] = data[AGENTS_ONLINE] + data[AGENTS_TRAINING] + data[AGENTS_AUX]

    # Ordenar por 'Total_Agentes' y 'AGENTS_ONLINE'
    df_grouped = data.sort_values(['Total_Agentes', AGENTS_ONLINE], ascending=[False, False]) \
        .drop_duplicates(subset=[DATE, TIME_INTERVAL, TEAM], keep='first')

    # Limpiar la columna 'TEAM'
    df_grouped[TEAM] = df_grouped[TEAM].str.strip().str.upper()

    return df_grouped
