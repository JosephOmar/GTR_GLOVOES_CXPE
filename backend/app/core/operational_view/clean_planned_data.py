import pandas as pd
from datetime import datetime, timedelta
from app.core.utils.real_data_view.columns_names import TEAM, DATE, TIME_INTERVAL, FORECAST_RECEIVED, FORECAST_AHT, FORECAST_ABSENTEEISM, REQUIRED_AGENTS, SCHEDULED_AGENTS, FORECAST_HOURS, SCHEDULED_HOURS

COLUMNS_PLANNED_DATA = {
    'CANAL': TEAM,
    'Fecha': DATE,
    'Intervalo': TIME_INTERVAL,
    'Pronostico-Recibidas': FORECAST_RECEIVED,
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

def clean_planned_data(data: pd.DataFrame) -> pd.DataFrame:
    data = data.rename(columns=COLUMNS_PLANNED_DATA)
    data = data[list(COLUMNS_PLANNED_DATA.values())]
    data = data[data[TEAM].isin(list(COLUMNS_TEAMS.keys()))]

    # Convertimos a date
    data[DATE] = pd.to_datetime(data[DATE]).dt.date

    # Definimos rango: 2 días antes → 1 día después
    current_day = datetime.today().date()
    start_date = current_day - timedelta(days=2)
    end_date = current_day + timedelta(days=1)

    # Filtramos por rango de fechas
    mask = (data[DATE] >= start_date) & (data[DATE] <= end_date)
    data = data.loc[mask]

    # Formateo de TIME_INTERVAL, incluyendo segundos
    data[TIME_INTERVAL] = pd.to_datetime(
        data[TIME_INTERVAL],
        format='%H:%M:%S',
        errors='coerce'
    ).dt.time

    if data[TIME_INTERVAL].isnull().all():
        raise ValueError(
            "La columna TIME_INTERVAL no pudo ser convertida a datetime.")

    data[TIME_INTERVAL] = data[TIME_INTERVAL].apply(
        lambda x: x.strftime('%H:%M') if pd.notnull(x) else None
    )

    # Reemplazo de equipos y cálculo de horas
    data[TEAM] = data[TEAM].replace(COLUMNS_TEAMS)
    data[FORECAST_HOURS] = data[REQUIRED_AGENTS].round().astype(int) * 0.5
    data[SCHEDULED_HOURS] = data[SCHEDULED_AGENTS] * 0.5

    return data