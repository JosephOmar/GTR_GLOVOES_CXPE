import pandas as pd
from app.core.utils.workers_cx.columns_names import TIME_LOGGED, KUSTOMER_ID, KUSTOMER_EMAIL, KUSTOMER_NAME, RECENT_LOGIN, DATE_LOGIN
from app.core.utils.workers_cx.utils import clean_worker_name, convert_ms_to_hours, convert_login_time_to_local_timezone

COLUMNS_REPORT = {
    "Name": KUSTOMER_NAME,
    "Total Time Logged In (ms)": TIME_LOGGED,
    "User ID": KUSTOMER_ID,
    "User Email": KUSTOMER_EMAIL,
    "Most Recent Login": RECENT_LOGIN,
}

def clean_api_id(data: pd.DataFrame) -> pd.DataFrame:
    # 🔹 Filtro: eliminar emails con @dyglovo y TIME_LOGGED = 0
    data = data[~(
        data["User Email"].str.contains(r'@dyglovo', na=False)
    )]

    # 0) Crear prioridades en lugar de eliminar
    data['prioridad_dylat'] = data['Name'].str.contains(r'\(DYLAT', na=False)
    data['prioridad_email'] = data["User Email"].str.contains(r'@providers\.glovoapp\.com', na=False)

    # 1) Ordenar por prioridades
    data = data.sort_values(
        by=['prioridad_dylat', 'prioridad_email'],
        ascending=[False, False]  
    ).reset_index(drop=True)

    # 2) Renombrar
    data = data.rename(columns=COLUMNS_REPORT)

    # 3) Limpiar nombre
    data[KUSTOMER_NAME] = data[KUSTOMER_NAME].apply(clean_worker_name)

    # 4) Convertir ms → horas
    data[TIME_LOGGED] = data[TIME_LOGGED].apply(convert_ms_to_hours)

    # 5) Partir el login reciente
    data[[RECENT_LOGIN, DATE_LOGIN]] = data[RECENT_LOGIN].apply(
        lambda x: pd.Series(convert_login_time_to_local_timezone(x))
    )

    # 6) Si hay nombres repetidos, quedarnos con el de mayor tiempo (gracias al orden)
    data = data.drop_duplicates(subset=[KUSTOMER_NAME], keep='first')

    # 7) Seleccionar columnas finales
    columns_to_keep = [KUSTOMER_NAME, TIME_LOGGED, KUSTOMER_ID, KUSTOMER_EMAIL, RECENT_LOGIN, DATE_LOGIN]
    data = data[columns_to_keep].reset_index(drop=True)
    return data

