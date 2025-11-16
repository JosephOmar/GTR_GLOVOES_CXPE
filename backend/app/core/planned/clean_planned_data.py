import pandas as pd
from datetime import datetime, timedelta
import math
from app.core.utils.planned.columns_names import (
    TEAM, DATE, INTERVAL, FORECAST_RECEIVED, REQUIRED_AGENTS, SCHEDULED_AGENTS, FORECAST_THT
)

COLUMNS_PLANNED_DATA = {
    'Servicio': TEAM,
    'Fecha': DATE,
    'Intervalo': INTERVAL,
    'Pronostico-THT' : FORECAST_THT,
    'Pronostico-Recibidas': FORECAST_RECEIVED,
    'RAC_s Planificados Disponibles': REQUIRED_AGENTS,
    'Programado Disponible + Ubycall/ Schedule workload': SCHEDULED_AGENTS,
}

COLUMNS_TEAMS = {
    'CUSTOMER TIER1': 'Customer Tier1',
    'CUSTOMER TIER2': 'Customer Tier2',
    'RIDER TIER1': 'Rider Tier1',
    'RIDER TIER2': 'Rider Tier2',
    'VENDOR TIER1': 'Vendor Tier1',
    'VENDOR TIER2': 'Vendor Tier2',
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

    data = data.rename(columns=COLUMNS_PLANNED_DATA)

    data = data[list(COLUMNS_PLANNED_DATA.values())]

    data = data[data[TEAM].isin(COLUMNS_TEAMS.keys())]

    data[DATE] = pd.to_datetime(data[DATE], errors='coerce').dt.date

    current_day = datetime.today().date()
    start_date = current_day - timedelta(days=1)
    end_date = current_day + timedelta(days=6)
    mask = (data[DATE] >= start_date) & (data[DATE] <= end_date)
    data = data.loc[mask]

    data[INTERVAL] = data[INTERVAL].apply(normalize_time)

    data[INTERVAL] = data[INTERVAL].apply(round_to_hour)

    data[INTERVAL] = data[INTERVAL].fillna("00:00")

    data[TEAM] = data[TEAM].replace(COLUMNS_TEAMS)

    data[REQUIRED_AGENTS] = data[REQUIRED_AGENTS].apply(lambda x: round(x))

    agg_funcs = {
        FORECAST_RECEIVED: "sum",
        REQUIRED_AGENTS: "max",
        SCHEDULED_AGENTS: "max",
        FORECAST_THT: "first", 
    }

    data = (
        data.groupby([TEAM, DATE, INTERVAL], as_index=False)
            .agg(agg_funcs)
    )

    return data
