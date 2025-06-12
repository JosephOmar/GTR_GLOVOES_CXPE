import pandas as pd
from datetime import datetime, timedelta
from app.core.utils.workers_cx.utils import update_column_based_on_worker
from app.core.utils.workers_cx.columns_names import DOCUMENT, ROLE, STATUS, CAMPAIGN, TEAM, MANAGER, SUPERVISOR, COORDINATOR, CONTRACT_TYPE, START_DATE, TERMINATION_DATE, WORK_TYPE, REQUIREMENT_ID, EMPLOYEE_NAME, FATHER_LAST_NAME, MOTHER_LAST_NAME, NAME, TENURE, TRAINEE
from app.core.utils.workers_cx.columns_names import CHAT_CUSTOMER, CHAT_RIDER, MAIL_CUSTOMER, MAIL_RIDER, MAIL_VENDORS, MAIL_CUSTOMER_MC, MAIL_CUSTOMER_IS, CALL_VENDORS, GLOVO_SPAIN, GERENCIA 
# Diccionario para traducir las columnas
COLUMNS_PEOPLE_CONSULTATION = {
    "NRO. DOCUMENTO": DOCUMENT,
    "CARGO": ROLE,
    "ESTADO": STATUS,
    "SERVICIO": CAMPAIGN,
    "DETALLE SERVICIO": TEAM,
    "GERENTE": MANAGER,
    "SUPERVISOR": SUPERVISOR,
    "RESPONSABLE": COORDINATOR,
    "TIPO CONTRATO": CONTRACT_TYPE,
    "FECHA ING.": START_DATE,
    "FECHA CESE": TERMINATION_DATE,
    "TIPO TRABAJO": WORK_TYPE,
    "ID REQUERIMIENTO": REQUIREMENT_ID,
    "NOMBRE EMPLEADO": EMPLOYEE_NAME,  
    "APELLIDO PATERNO ":FATHER_LAST_NAME, 
    "APELLIDO MATERNO": MOTHER_LAST_NAME
}

COLUMNS_TEAM = {
    "CHAT USER ESPAÑA": CHAT_CUSTOMER,
    "ID VERIFY + PAYMENT ES ": CHAT_CUSTOMER,
    "CHAT GLOVER ESPAÑA": CHAT_RIDER,
    "MAIL PARTNER": MAIL_VENDORS,
    "PARTNERCALL ESPAÑA": CALL_VENDORS,
    "GERENCIA 1": GERENCIA,
    "MAIL GLOVER ESPAÑA": MAIL_RIDER,
    "PARTNER ONLINECALLS": CALL_VENDORS,
    "MAIL USER ESPAÑA": MAIL_CUSTOMER,
    "GLOVO": GLOVO_SPAIN,
    "AUDITORIA MCDONALDS": MAIL_CUSTOMER_MC,
    "GLOVO ESPAÑA": GLOVO_SPAIN,
    "INCIDENTES SEVEROS ESPAÑA":MAIL_CUSTOMER_IS,
}

def clean_people_consultation(data: pd.DataFrame) -> pd.DataFrame:
    
    # Renombrar las columnas usando el diccionario de traducción
    data = data.rename(columns=COLUMNS_PEOPLE_CONSULTATION)

    current_date = datetime.now()
    fecha_limite = current_date - timedelta(days=30)

    cond_activo = data[STATUS].str.lower() == 'activo'
    cond_inactivo_reciente = (
        (data[STATUS].str.lower() == 'inactivo') &
        (pd.to_datetime(data[TERMINATION_DATE], errors='coerce') >= fecha_limite)
    )

    data = data[cond_activo | cond_inactivo_reciente]

    # Filtrar solo workers con team requerida
    team_values = list(COLUMNS_TEAM.keys())
    data = data[data[TEAM].isin(team_values)]

    # Renombrar Team
    data[TEAM] = data[TEAM].replace(COLUMNS_TEAM)

    # Filtrar los datos primero
    data = data[(data[CAMPAIGN] == 'GLOVO') | ((data[CAMPAIGN] == 'GERENCIA 1') & (data[ROLE] == 'GERENTE DE OPERACIONES'))]

    # Verificar si los apellidos están presentes en 'employee_name', si es así, evitar duplicarlos
    data[NAME] = data.apply(lambda row: f"{row[EMPLOYEE_NAME]} {row[FATHER_LAST_NAME]} {row[MOTHER_LAST_NAME]}".strip()
                               if row[FATHER_LAST_NAME] not in row[EMPLOYEE_NAME] and row[MOTHER_LAST_NAME] not in row[EMPLOYEE_NAME]
                               else row[EMPLOYEE_NAME], axis=1)

    # Normalizar los nombres
    data[NAME] = data[NAME].str.title()
    data[MANAGER] = data[MANAGER].str.title().str.strip()
    data[SUPERVISOR] = data[SUPERVISOR].str.title().str.strip()
    data[COORDINATOR] = data[COORDINATOR].str.title().str.strip()

    # Corregir el orden de los nombres en las columnas 'manager', 'supervisor' y 'coordinator'
    data = update_column_based_on_worker(data, data, MANAGER, NAME)
    data = update_column_based_on_worker(data, data, SUPERVISOR, NAME)
    data = update_column_based_on_worker(data, data, COORDINATOR, NAME)

    # Convertir las fechas a formato datetime
    data[START_DATE] = pd.to_datetime(data[START_DATE], errors='coerce')
    data[TERMINATION_DATE] = pd.to_datetime(data[TERMINATION_DATE], errors='coerce')

    # Calcular la antigüedad en meses (basado en 'start_date')
    # Fecha actual
    current_date = datetime.now()

    # Cálculo de TENURE con la condición de asignar 0 cuando la antigüedad es menor a un mes completo
    data[TENURE] = data[START_DATE].apply(
        lambda x: 0 if (current_date.year == x.year and current_date.month == x.month) or 
                (current_date.year == x.year and current_date.month - x.month == 1 and current_date.day < x.day) else
                (current_date.year - x.year) * 12 + current_date.month - x.month
    )

    # Crear la columna 'trainee'
    data[TRAINEE] = data[TENURE].apply(lambda x: "DESPEGANDO" if pd.notnull(x) and x < 1 else None)

    # *Formateamos los documentos para quitar los 0 iniciales, y tener concordancia con los otros archivos
    data[DOCUMENT] = data[DOCUMENT].astype(str).str.lstrip("0").astype(int)

    # Opcional: Eliminar las columnas que no necesitas para mantener solo las que quieres almacenar
    columns_to_keep = [DOCUMENT, NAME, ROLE, STATUS, CAMPAIGN, TEAM, MANAGER, 
                       SUPERVISOR, COORDINATOR, CONTRACT_TYPE, START_DATE, TERMINATION_DATE, WORK_TYPE, REQUIREMENT_ID, TENURE, TRAINEE]
    data = data[columns_to_keep]

    return data
