import pandas as pd
from datetime import datetime, timedelta
from app.core.utils.real_data_view.columns_names import (
    TEAM, DATE, TIME_INTERVAL, FORECAST_RECEIVED, FORECAST_AHT, 
    FORECAST_ABSENTEEISM, REQUIRED_AGENTS, SCHEDULED_AGENTS, 
    FORECAST_HOURS, SCHEDULED_HOURS
)

COLUMNS_PLANNED_DATA = {
    'CANAL': TEAM,
    'Fecha': DATE,
    'Intervalo': TIME_INTERVAL,
    'Pronostico-Recibidas': 'interval',
    'Pronostico-TMO': FORECAST_AHT,
    'Pronostico-Ausentismo': FORECAST_ABSENTEEISM,
    'RAC_s Planificados Disponibles': REQUIRED_AGENTS,
    'Programados sin ausentismo + Ubycall': SCHEDULED_AGENTS,
}

COLUMNS_TEAMS = {
    'CHAT GLOVER': 'CHAT RIDER',
    'CHAT USER': 'CHAT CUSTOMER',
    'PARTNERCALL': 'CALL VENDORS'
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

def clean_planned_data(data: pd.DataFrame) -> pd.DataFrame:
    # Renombramos columnas
    data = data.rename(columns=COLUMNS_PLANNED_DATA)
    data = data[list(COLUMNS_PLANNED_DATA.values())]
    
    # Filtramos por equipos
    data = data[data[TEAM].isin(COLUMNS_TEAMS.keys())]
    
    # Convertimos a tipo fecha
    data[DATE] = pd.to_datetime(data[DATE], errors='coerce').dt.date
    
    # Rango de fechas: 2 días antes → 1 día después
    current_day = datetime.today().date()
    start_date = current_day - timedelta(days=2)
    end_date = current_day + timedelta(days=1)
    mask = (data[DATE] >= start_date) & (data[DATE] <= end_date)
    data = data.loc[mask]
    
    # Normalizamos TIME_INTERVAL
    data[TIME_INTERVAL] = data[TIME_INTERVAL].apply(normalize_time)
    
    # Si después de la normalización hay valores vacíos, los llenamos con "00:00"
    data[TIME_INTERVAL] = data[TIME_INTERVAL].fillna("00:00")
    
    # Reemplazo de nombres de equipos
    data[TEAM] = data[TEAM].replace(COLUMNS_TEAMS)
    
    # Cálculo de horas
    data[FORECAST_HOURS] = data[REQUIRED_AGENTS].round().astype(int) * 0.5
    data[SCHEDULED_HOURS] = data[SCHEDULED_AGENTS] * 0.5
    
    # Filtramos por fecha específica (opcional)
    data = data[data[DATE].astype(str) == '2025-07-29']

    return data
