import pandas as pd
from app.core.utils.workers_cx.columns_names import TIME_LOGGED, KUSTOMER_ID, KUSTOMER_EMAIL, KUSTOMER_NAME, RECENT_LOGIN, DATE_LOGIN
from app.core.utils.workers_cx.utils import clean_worker_name, convert_ms_to_hours, convert_login_time_to_local_timezone
from rapidfuzz import process

# Diccionario para traducir las columnas
COLUMNS_REPORT = {
    "Name": KUSTOMER_NAME,
    "Total Time Logged In (ms)": TIME_LOGGED,
    "User ID": KUSTOMER_ID,
    "User Email": KUSTOMER_EMAIL,
    "Most Recent Login": RECENT_LOGIN
}

# Función de limpieza para 'Report'
def clean_report_kustomer(data: pd.DataFrame) -> pd.DataFrame:
    # Agregar columna auxiliar para priorizar nombres que contengan '(DYLAT'
    data['prioridad_dylat'] = data['Name'].str.contains(r'\(DYLAT', na=False)

    # Ordenar poniendo primero los que contienen '(DYLAT'
    data = data.sort_values(by='prioridad_dylat', ascending=False).reset_index(drop=True)

    # Renombrar las columnas usando el diccionario de traducción
    data = data.rename(columns=COLUMNS_REPORT)

    # Limpiar la columna 'worker' (nombre)
    data[KUSTOMER_NAME] = data[KUSTOMER_NAME].apply(clean_worker_name)
    
    # Convertir el tiempo de milisegundos a horas en la columna 'time_logged'
    data[TIME_LOGGED] = data[TIME_LOGGED].apply(convert_ms_to_hours)
    
    # Separar la columna 'recent_login' en 'recent_login' y 'date'
    data[[RECENT_LOGIN, DATE_LOGIN]] = data[RECENT_LOGIN].apply(lambda x: pd.Series(convert_login_time_to_local_timezone(x)))
    
    # Opcional: Eliminar las columnas originales que ya no necesitamos
    columns_to_keep = [KUSTOMER_NAME, TIME_LOGGED, KUSTOMER_ID, KUSTOMER_EMAIL, RECENT_LOGIN, DATE_LOGIN]
    data = data[columns_to_keep]

    return data
