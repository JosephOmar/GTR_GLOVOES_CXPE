import pandas as pd
from datetime import datetime
import numpy as np

# Importar las variables
from app.core.utils.workers_cx.columns_names import DOCUMENT, NAME, STATUS, START_DATE, KUSTOMER_EMAIL, SUPERVISOR, COORDINATOR, TEAM, TENURE, CHAT_CUSTOMER, CHAT_RIDER, CALL_VENDORS
from app.core.workers_concentrix.clean_people_consultation import clean_people_consultation
from app.core.utils.workers_cx.utils import update_column_based_on_worker

COLUMNS_MASTER_GLOVO = {
    "DNI": DOCUMENT,
    "NOMBRE": NAME,
    "ESTADO": STATUS,
    "FECHA DE ALTA": START_DATE,
    "Usuario Kustomer": KUSTOMER_EMAIL,
    "SUPERVISOR": SUPERVISOR,
    "RESPONSABLE": COORDINATOR,
    "CANALES GLOVO": TEAM
}


def clean_master_glovo(data: pd.DataFrame, people_active: pd.DataFrame, people_inactive) -> pd.DataFrame:

    data_people = clean_people_consultation(people_active, people_inactive)

    data = data.rename(columns=COLUMNS_MASTER_GLOVO)
    # Eliminar ceros iniciales en 'DOCUMENT'
    data[DOCUMENT] = (data[DOCUMENT].astype(str).str.lstrip("0").replace({'nan': '0', '': '0'})).astype(int)

    # Capitalizar los nombres en 'NAME', 'SUPERVISOR', 'RESPONSABLE'
    data[NAME] = data[NAME].apply(
        lambda x: x.strip().title() if isinstance(x, str) else x)
    data[SUPERVISOR] = data[SUPERVISOR].apply(
        lambda x: x.strip().title() if isinstance(x, str) else x)
    data[COORDINATOR] = data[COORDINATOR].apply(
        lambda x: x.strip().title() if isinstance(x, str) else x)

    # Reemplazar valores en 'TEAM'
    data[TEAM] = data[TEAM].replace(
        {'Ubycall Chat User': CHAT_CUSTOMER, 'Ubycall Chat Glover': CHAT_RIDER, 'Ubycall Partnercall Es': CALL_VENDORS})
    data = data[data[TEAM].isin([CHAT_CUSTOMER, CHAT_RIDER, CALL_VENDORS])]
    data = update_column_based_on_worker(data, data_people, SUPERVISOR, NAME)
    data = update_column_based_on_worker(data, data_people, COORDINATOR, NAME)

    data[START_DATE] = pd.to_datetime(data[START_DATE], errors='coerce')

    # Crear la columna 'TENURE' en meses desde la fecha de 'START_DATE'
    current_date = datetime.now()
    # Cálculo de TENURE con la condición de asignar 0 cuando la antigüedad es menor a un mes completo
    data[TENURE] = data[START_DATE].apply(
        lambda x: 0 if (current_date.year == x.year and current_date.month == x.month) or
        (current_date.year == x.year and current_date.month - x.month == 1 and current_date.day < x.day) else
        (current_date.year - x.year) * 12 + current_date.month - x.month
    )
    data[STATUS] = data[STATUS].str.capitalize()
    data[KUSTOMER_EMAIL] = data[KUSTOMER_EMAIL].str.lower()
    return data
