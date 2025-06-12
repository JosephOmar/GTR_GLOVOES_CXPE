import pandas as pd
from app.core.utils.real_data_view.columns_names import TEAM, TIME_INTERVAL, SAT_SAMPLES, SAT_ONGOING, SAT_INTERVAL, SAT_PROMOTERS

COLUMNS_KUSTOMER_DATA = {
    'DateTime': TIME_INTERVAL,
    'Average': SAT_INTERVAL,
}

def clean_kustomer_data(
    data_partial_CSAT: pd.DataFrame,
    data_total_CSAT: pd.DataFrame,
    data_partial_RSAT: pd.DataFrame,
    data_total_RSAT: pd.DataFrame
) -> pd.DataFrame:
    # Asignar equipo a los parciales
    data_partial_CSAT[TEAM] = 'CHAT CUSTOMER'
    data_partial_RSAT[TEAM] = 'CHAT RIDER'
    print(data_total_CSAT)
    # --- CSAT usando Chat + Email ---
    # Verificar si existen las columnas 'Chat' y 'Email'; si no, crearlas con ceros
    for col in ['Chat', 'Email']:
        if col not in data_total_CSAT.columns:
            data_total_CSAT[col] = 0

    # Ya debe existir 'Sendbird', 'Chat' y 'Email' (o haberse creado con ceros)
    data_total_CSAT['Total_CSAT'] = data_total_CSAT[[
        'Sendbird', 'Chat', 'Email']].sum(axis=1)

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