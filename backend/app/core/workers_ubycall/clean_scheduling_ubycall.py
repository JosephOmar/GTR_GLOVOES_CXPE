import pandas as pd
from datetime import datetime
import numpy as np

# Importar las variables
from app.core.utils.workers_cx.columns_names import DOCUMENT, NAME, STATUS, START_DATE, KUSTOMER_EMAIL, SUPERVISOR, COORDINATOR, TEAM, TENURE, CHAT_CUSTOMER, CHAT_RIDER, CALL_VENDORS

COLUMNS_SCHEDULING_UBYCALL = {
    "DNI": DOCUMENT,
    "NOMBRECOMPLETO": NAME,
    "HORARIOSELECCIONADO": STATUS,
    #"FECHA_CREA_AGENTE": START_DATE,
    # "CORREO": KUSTOMER_EMAIL,
    "CAMPANA": TEAM
}


def clean_scheduling_ubycall(data: pd.DataFrame) -> pd.DataFrame:

    data = data.rename(columns=COLUMNS_SCHEDULING_UBYCALL)
    # Paso 1: Limpiar la columna 'DNI' eliminando los ceros a la izquierda
    data[DOCUMENT] = data[DOCUMENT].astype(str).str.lstrip("0").astype(int)

    # Paso 2: Capitalizar los nombres en 'NOMBRECOMPLETO'
    data[NAME] = data[NAME].str.title()

    data = data.drop_duplicates(subset=[NAME])

    # Paso 3: Reemplazar los valores en la columna 'CAMPANA' (TEAM)
    data[TEAM] = data[TEAM].replace({
        'GLOVO -  GLOVER ESPANA': 'RIDER TIER1',
        'GLOVO -  USER ESPANA': 'CUSTOMER TIER1',
        'GLOVO - USER TIER C': 'CUSTOMER TIER1',
        'GLOVO - PARTNER SERVICES': 'VENDOR CALL'
    })
    data = data[data[TEAM].isin(['CUSTOMER TIER1', 'RIDER TIER1', 'VENDOR CALL'])]

    # Paso 4: Transformar 'START_DATE' de formato 'YYYYMMDD' a datetime
    # data[START_DATE] = pd.to_datetime(data[START_DATE], format='%Y%m%d')

    # # Paso 5: Calcular la antigüedad en meses (TENURE)
    # # Fecha actual
    # current_date = datetime.now()

    # Calcular la antigüedad en meses
    # data[TENURE] = data[START_DATE].apply(
    #     lambda x: 0 if (current_date.year == x.year and current_date.month == x.month) or
    #     (current_date.year == x.year and current_date.month - x.month == 1 and current_date.day < x.day) else
    #     (current_date.year - x.year) * 12 + current_date.month - x.month
    # )

    data[SUPERVISOR] = np.nan
    data[COORDINATOR] = np.nan
    data[STATUS] = data[STATUS].str.capitalize()

    return data
