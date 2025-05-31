import pandas as pd
from datetime import datetime
from app.core.utils.data_kpis.column_names import TEAM, WEEK, DATE, TIME_INTERVAL, FORECAST_RECEIVED, FORECAST_AHT, FORECAST_ABSENTEEISM, REQUIRED_AGENTS, SCHEDULED_CONCENTRIX, SCHEDULED_UBYCALLS

COLUMNS_DATA_PLANNED = {
    'CANAL' : TEAM,
    'SEMANA' : WEEK,
    'Fecha' : DATE,
    'Intervalo' : TIME_INTERVAL,
    'Pronostico-Recibidas' : FORECAST_RECEIVED,
    'Pronostico-TMO' : FORECAST_AHT,
    'Pronostico-Ausentismo' : FORECAST_ABSENTEEISM,
    'RAC_s Planificados Disponibles' : REQUIRED_AGENTS,
    'RAC_s Programados Disponibles' : SCHEDULED_CONCENTRIX,
    'UBYCALL' : SCHEDULED_UBYCALLS
}

def clean_planned_data(data: pd.DataFrame) -> pd.DataFrame:

    data = data.rename(columns=COLUMNS_DATA_PLANNED)

    data = data[list(COLUMNS_DATA_PLANNED.values())]

    data[DATE] = pd.to_datetime(data[DATE], errors="coerce")

    data[WEEK] = data[DATE].dt.isocalendar().week

    current_week = datetime.today().isocalendar().week
    next_week = current_week + 1

    data = data[data[WEEK].isin([current_week, next_week])]

    data["forecast_hours"] = data[REQUIRED_AGENTS].round().astype(int) * 0.5
    data["scheduled_hours"] = (data[SCHEDULED_CONCENTRIX] + data[SCHEDULED_UBYCALLS]) * 0.5

    return data

