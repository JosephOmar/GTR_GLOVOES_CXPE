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

    # Asegúrate de que DATE esté en formato datetime, incluyendo la hora
    data[DATE] = pd.to_datetime(data[DATE], format='%d/%m/%Y %H:%M:%S', errors='coerce')  # Convertir a datetime sin zona horaria

    # Eliminar zona horaria si está presente antes de convertir
    if data[DATE].dt.tz is not None:
        data[DATE] = data[DATE].dt.tz_localize(None)

    # Localizar la fecha en la zona horaria de Perú
    data[DATE] = data[DATE].dt.tz_localize(peru_tz)

    # Convertir a la zona horaria de Madrid
    data[DATE] = data[DATE].dt.tz_convert(madrid_tz)

    # Eliminar la zona horaria después de la conversión (si no la necesitas)
    data[DATE] = data[DATE].dt.tz_localize(None)  # Esto elimina la zona horaria

    # Extraer la fecha y redondear a 30 minutos
    data[TIME_INTERVAL] = data[DATE].dt.floor('30min').dt.strftime('%H:%M')

    data[DATE] = pd.to_datetime(data[DATE]).dt.date
    # Imprimir para verificar que las fechas sean correctas

    # Sumar las columnas de agentes
    data['Total_Agentes'] = data[AGENTS_ONLINE] + data[AGENTS_TRAINING] + data[AGENTS_AUX]

    # Ordenar por 'Total_Agentes' y en caso de empate por 'AGENTS_ONLINE'
    df_grouped = data.sort_values(['Total_Agentes', AGENTS_ONLINE], ascending=[False, False]) \
        .drop_duplicates(subset=[DATE, TIME_INTERVAL, TEAM], keep='first')

    # Limpiar la columna 'TEAM' de espacios extra y convertir a mayúsculas
    df_grouped[TEAM] = df_grouped[TEAM].str.strip().str.upper()

    return df_grouped
