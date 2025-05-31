import pandas as pd
from datetime import datetime, timedelta
import pytz
from app.core.utils.real_data_view.columns_names import TEAM, DATE, TIME_INTERVAL, FORECAST_RECEIVED, FORECAST_AHT, FORECAST_ABSENTEEISM, REQUIRED_AGENTS, SCHEDULED_AGENTS, FORECAST_HOURS, SCHEDULED_HOURS, SERVICE_LEVEL, REAL_RECEIVED, SAT_SAMPLES, SAT_ONGOING, SAT_INTERVAL, SAT_PROMOTERS, REAL_AGENTS

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

COLUMNS_ASSEMBLED_DATA = {
    'Start Time': TIME_INTERVAL,
    'Queue': TEAM,
    'Service Level - Actual': SERVICE_LEVEL,
    'Contacts Received - Actual': REAL_RECEIVED,
}

COLUMNS_KUSTOMER_DATA = {
    'DateTime': TIME_INTERVAL,
    'Average': SAT_INTERVAL,
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
    print(data)
    return data


def clean_assembled_data(data_chat: pd.DataFrame, data_call: pd.DataFrame) -> pd.DataFrame:
    # 1) Concatenar
    data = pd.concat([data_chat, data_call], ignore_index=True)

    # 2) Renombrar columnas
    data = data.rename(columns=COLUMNS_ASSEMBLED_DATA)

    # 3) Extraer fecha y hora de la columna TIME_INTERVAL
    #    — Primero convertimos a datetime completo (fecha + hora)
    data['__DATETIME__'] = pd.to_datetime(data[TIME_INTERVAL], errors='coerce')

    #    — Nueva columna DATE con sólo la fecha (tipo datetime.date)
    data[DATE] = data['__DATETIME__'].dt.date

    #    — Reformatear TIME_INTERVAL a '%H:%M'
    data[TIME_INTERVAL] = data['__DATETIME__'].dt.strftime('%H:%M')

    # 4) Ya no necesitamos el auxiliar
    data = data.drop(columns='__DATETIME__')

    # 5) Convertir servicio y contactos
    data[SERVICE_LEVEL] = data[SERVICE_LEVEL].str.rstrip('%').astype(float)

    # 6) Merge de colas “customer”
    merge_queues = ['Spain Customer Verify ID', 'Spain Customers']
    data_customer = data[data[TEAM].isin(merge_queues)].copy()
    data_others = data[~data[TEAM].isin(merge_queues)].copy()

    data_customer = (
        data_customer
        # puedes agrupar por fecha y hora si lo deseas
        .groupby([DATE, TIME_INTERVAL])
        .apply(lambda grp: pd.Series({
            REAL_RECEIVED: grp[REAL_RECEIVED].sum(),
            SERVICE_LEVEL: (grp[SERVICE_LEVEL] * grp[REAL_RECEIVED]
                            ).sum() / grp[REAL_RECEIVED].sum()
        }))
        .reset_index()
    )
    data_customer[TEAM] = 'CHAT CUSTOMER'

    # 7) Renombrar otros equipos
    data_others[TEAM] = data_others[TEAM].replace({
        'Spain Glovers': 'CHAT RIDER',
        'Spain Partners': 'CALL VENDORS'
    })

    # 8) Concatenar resultado final
    data_final = pd.concat([data_customer, data_others], ignore_index=True)

    # 9) Redondeo y reordenar columnas
    data_final[SERVICE_LEVEL] = data_final[SERVICE_LEVEL].round(2)
    data_final = data_final[[DATE, TIME_INTERVAL,
                             TEAM, REAL_RECEIVED, SERVICE_LEVEL]]

    return data_final


import pandas as pd

def clean_kustomer_data(
    data_partial_CSAT: pd.DataFrame,
    data_total_CSAT: pd.DataFrame,
    data_partial_RSAT: pd.DataFrame,
    data_total_RSAT: pd.DataFrame
) -> pd.DataFrame:
    # Asignar equipo a los parciales
    data_partial_CSAT[TEAM] = 'CHAT CUSTOMER'
    data_partial_RSAT[TEAM] = 'CHAT RIDER'

    # --- CSAT usando Chat + Email ---
    # Verificar si existen las columnas 'Chat' y 'Email'; si no, crearlas con ceros
    for col in ['Chat', 'Email']:
        if col not in data_total_CSAT.columns:
            data_total_CSAT[col] = 0

    # Ya debe existir 'Sendbird', 'Chat' y 'Email' (o haberse creado con ceros)
    data_total_CSAT['Total_CSAT'] = data_total_CSAT[['Sendbird', 'Chat', 'Email']].sum(axis=1)

    total_CSAT = data_total_CSAT['Total_CSAT'].sum()
    # Promotores: categorías 4 y 5
    total_promotors_CSAT = data_total_CSAT.loc[
        data_total_CSAT['Category'].isin([4, 5]),
        'Total_CSAT'
    ].sum()

    current_CSAT = ((total_promotors_CSAT / total_CSAT) * 100).round(2)
    ongoing_CSAT = ((5 * current_CSAT) / 100).round(2)

    # --- RSAT usando Sendbird ---
    total_RSAT = data_total_RSAT['Sendbird'].sum()
    total_promotors_RSAT = data_total_RSAT.loc[
        data_total_RSAT['Category'].isin([4, 5]),
        'Sendbird'
    ].sum()

    current_RSAT = ((total_promotors_RSAT / total_RSAT) * 100).round(2)
    ongoing_RSAT = ((5 * current_RSAT) / 100).round(2)

    # --- Agregar resultados a los parciales ---
    data_partial_CSAT[SAT_SAMPLES] = total_CSAT
    data_partial_RSAT[SAT_SAMPLES] = total_RSAT

    data_partial_CSAT[SAT_PROMOTERS] = current_CSAT
    data_partial_RSAT[SAT_PROMOTERS] = current_RSAT

    data_partial_CSAT[SAT_ONGOING] = ongoing_CSAT
    data_partial_RSAT[SAT_ONGOING] = ongoing_RSAT

    # --- Concatenar, formatear e indexar ---
    data = pd.concat([data_partial_CSAT, data_partial_RSAT], ignore_index=True)
    data = data.rename(columns=COLUMNS_KUSTOMER_DATA)

    # Formatear porcentaje de intervalo de satisfacción
    data[SAT_INTERVAL] = (data[SAT_INTERVAL] * 100).round(2)

    # Formatear tiempo
    data[TIME_INTERVAL] = pd.to_datetime(data[TIME_INTERVAL])
    data[TIME_INTERVAL] = data[TIME_INTERVAL].dt.strftime('%H:%M')

    # Seleccionar columnas finales
    data = data[[TEAM, TIME_INTERVAL, SAT_INTERVAL,
                 SAT_SAMPLES, SAT_PROMOTERS, SAT_ONGOING]]

    return data




def clean_real_agents(data: pd.DataFrame) -> pd.DataFrame:
    peru_tz = pytz.timezone('America/Lima')
    madrid_tz = pytz.timezone('Europe/Madrid')

    # Convertir a datetime con zona horaria
    data['Marca temporal'] = pd.to_datetime(data['Marca temporal'], dayfirst=True).dt.tz_localize(peru_tz)
    data['Marca temporal'] = data['Marca temporal'].dt.tz_convert(madrid_tz)

    # Extraer fecha y redondear a 30 min
    data[DATE] = data['Marca temporal'].dt.date
    data[TIME_INTERVAL] = data['Marca temporal'].dt.floor('30min').dt.strftime('%H:%M')

    # Seleccionar el registro con mayor 'Agentes Conectados' por grupo
    df_grouped = data.sort_values('Agentes Conectados (Online + Aux)', ascending=False) \
        .drop_duplicates(subset=[DATE, TIME_INTERVAL, 'Seleccione Canal'], keep='first')

    # Renombrar columnas
    df_grouped = df_grouped.rename(columns={
        'Seleccione Canal': TEAM,
        'Agentes Conectados (Online + Aux)': REAL_AGENTS
    })

    df_grouped[TEAM] = df_grouped[TEAM].str.strip().str.upper()

    return df_grouped[[DATE, TIME_INTERVAL, TEAM, REAL_AGENTS]]


def merge_data_view(
    data_planned: pd.DataFrame,
    data_assembled_chat: pd.DataFrame,
    data_assembled_call: pd.DataFrame,
    data_kustomer_partial_CS: pd.DataFrame,
    data_kustomer_total_CS: pd.DataFrame,
    data_kustomer_partial_RD: pd.DataFrame,
    data_kustomer_total_RD: pd.DataFrame,
    data_real_agents: pd.DataFrame
) -> pd.DataFrame:

    # --- 1) Limpiar cada fuente ---
    data_planned = clean_planned_data(data_planned)
    data_assembled = clean_assembled_data(
        data_assembled_chat, data_assembled_call)
    data_kustomer = clean_kustomer_data(
        data_kustomer_partial_CS,
        data_kustomer_total_CS,
        data_kustomer_partial_RD,
        data_kustomer_total_RD
    )
    data_real_agents = clean_real_agents(data_real_agents)

    # --- 2) Merge de ensamblados, kustomer y real_agents por TIME_INTERVAL y TEAM ---
    df_temp = data_assembled.copy()
    df_temp = pd.merge(df_temp, data_kustomer,    on=[
                       TIME_INTERVAL, TEAM], how='outer')
    print(df_temp)
    df_temp = pd.merge(df_temp, data_real_agents, on=[DATE, TIME_INTERVAL, TEAM], how='outer')
    print(df_temp)
    # --- 3) Merge con data_planned por DATE (outer para conservar todas las planned dates) ---
    df_merged = pd.merge(
        data_planned,
        df_temp,
        on=[DATE, TIME_INTERVAL, TEAM],
        how='outer'
    )
    print(df_merged)
    print('holi')
    # --- 4) Ordenar y resetear índice ---
    df_merged = df_merged.sort_values(by=[DATE, TEAM, TIME_INTERVAL]) \
                         .reset_index(drop=True)
    print(df_merged)
    return df_merged
