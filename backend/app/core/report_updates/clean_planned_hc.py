import pandas as pd
from datetime import datetime, timedelta
from app.core.utils.real_data_view.columns_names import (
    TEAM, DATE, INTERVAL, FORECAST_RECEIVED, REQUIRED_AGENTS, SCHEDULED_AGENTS
)

COLUMNS_PLANNED_DATA = {
    'Servicio': TEAM,
    'Fecha': DATE,
    'Intervalo': INTERVAL,
    'Pronostico-Recibidas': FORECAST_RECEIVED,
    'RAC_s Planificados Logueados': REQUIRED_AGENTS,
    'Programado Logeado + Ubycall': SCHEDULED_AGENTS,
}

COLUMNS_TEAMS = {
    'CHAT GLOVER': 'CHAT RIDER HC',
    'CHAT USER': 'CHAT CUSTOMER HC',
    'PARTNERCALL': 'CALL VENDOR HC',
    'MAIL USER': 'RUBIK CUSTOMER',
    'MAIL GLOVER': 'RUBIK RIDER',
    'MAIL PARTNER' : 'RUBIK VENDOR'
}

def normalize_time(value: str) -> str:
    """
    Normaliza el valor de la columna Intervalo.
    - Si el valor es un rango 'HH:MM - HH:MM', toma la hora inicial.
    - Si el valor incluye segundos, recorta a HH:MM.
    - Si no se puede convertir, retorna None.
    """
    if pd.isnull(value):
        return None
    val = str(value).strip()
    if '-' in val:
        val = val.split('-')[0].strip()
    try:
        return datetime.strptime(val[:5], '%H:%M').strftime('%H:%M')
    except Exception:
        return None

def round_to_hour(value: str) -> str:
    """
    Redondea los intervalos de 30 min a la hora exacta hacia abajo.
    Ejemplo: 00:00 -> 00:00, 00:30 -> 00:00, 01:30 -> 01:00
    """
    try:
        t = datetime.strptime(value, "%H:%M")
        return t.replace(minute=0).strftime("%H:%M")
    except Exception:
        return None

def clean_planned_data(data: pd.DataFrame) -> pd.DataFrame:
    # Renombramos columnas
    data = data.rename(columns=COLUMNS_PLANNED_DATA)
    data = data[list(COLUMNS_PLANNED_DATA.values())]

    # Filtramos por equipos
    data = data[data[TEAM].isin(COLUMNS_TEAMS.keys())]

    # Convertimos a tipo fecha
    data[DATE] = pd.to_datetime(data[DATE], errors='coerce').dt.date

    # Rango de fechas: dia actual → 1 día después
    current_day = datetime.today().date()
    start_date = current_day
    end_date = current_day + timedelta(days=1)
    mask = (data[DATE] >= start_date) & (data[DATE] <= end_date)
    data = data.loc[mask]

    print(data)
    # Normalizamos TIME_INTERVAL
    data[INTERVAL] = data[INTERVAL].apply(normalize_time)

    # Si después de la normalización hay valores vacíos, los llenamos con "00:00"
    data[INTERVAL] = data[INTERVAL].fillna("00:00")

    # Reemplazo de nombres de equipos
    data[TEAM] = data[TEAM].replace(COLUMNS_TEAMS)

    # Agrupación por TEAM, DATE, HORA
    agg_funcs = {
        FORECAST_RECEIVED: "sum",
        REQUIRED_AGENTS: "max",
        SCHEDULED_AGENTS: "max",
    }

    data = (
        data.groupby([TEAM, DATE, INTERVAL], as_index=False)
            .agg(agg_funcs)
    )

    print(data[data[TEAM] == 'CHAT CUSTOMER HC'].head(30))

    return data
