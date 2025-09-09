import pandas as pd
from app.core.utils.workers_cx.columns_names import TIME_LOGGED, KUSTOMER_ID, KUSTOMER_EMAIL, KUSTOMER_NAME, RECENT_LOGIN, DATE_LOGIN
from app.core.utils.workers_cx.utils import clean_worker_name, convert_ms_to_hours, convert_login_time_to_local_timezone

COLUMNS_REPORT = {
    "Name": KUSTOMER_NAME,
    "Total Time Logged In (ms)": TIME_LOGGED,
    "User ID": KUSTOMER_ID,
    "User Email": KUSTOMER_EMAIL,
    "Most Recent Login": RECENT_LOGIN
}

def clean_report_kustomer(data: pd.DataFrame) -> pd.DataFrame:
    # 0) Eliminar registros con TIME_LOGGED = 0
    data = data[data["Total Time Logged In (ms)"] > 0].reset_index(drop=True)

    # 1) Prioridad por DYLAT y por tiempo (aún en ms, antes de renombrar)
    data['prioridad_dylat'] = data['Name'].str.contains(r'\(DYLAT', na=False)
    data = data.sort_values(
        by=['prioridad_dylat', 'Total Time Logged In (ms)'],
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


