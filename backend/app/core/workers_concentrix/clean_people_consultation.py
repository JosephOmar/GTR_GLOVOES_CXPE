import pandas as pd
from datetime import datetime, timedelta
from app.core.utils.workers_cx.utils import update_column_based_on_worker
from app.core.utils.workers_cx.columns_names import DOCUMENT, ROLE, STATUS, CAMPAIGN, TEAM, MANAGER, SUPERVISOR, COORDINATOR, CONTRACT_TYPE, START_DATE, TERMINATION_DATE, WORK_TYPE, REQUIREMENT_ID, EMPLOYEE_NAME, FATHER_LAST_NAME, MOTHER_LAST_NAME, NAME, TENURE, TRAINEE
from app.core.utils.workers_cx.columns_names import CHAT_CUSTOMER, CHAT_RIDER, MAIL_CUSTOMER, MAIL_RIDER, MAIL_VENDORS, CALL_VENDORS, GLOVO_SPAIN, GERENCIA, UPDATE
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
    "APELLIDO PATERNO ": FATHER_LAST_NAME,
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
    "INCIDENTES SEVEROS ESPAÑA" : MAIL_CUSTOMER,
    "MAIL USER ESPAÑA": MAIL_CUSTOMER,
    "GLOVO": GLOVO_SPAIN,
    "GLOVO ESPAÑA": GLOVO_SPAIN,
    "UPDATE": UPDATE
}


def clean_people_consultation(data_active: pd.DataFrame, data_inactive: pd.DataFrame) -> pd.DataFrame:

    data = pd.concat([data_active, data_inactive], ignore_index=True)
    # Renombrar las columnas usando el diccionario de traducción
    data = data.rename(columns=COLUMNS_PEOPLE_CONSULTATION)
    current_date = datetime.now()
    # Obtener el primer día del mes actual
    primer_dia_mes_actual = current_date.replace(day=1)

    if current_date.day <= 10:
        # Si está entre el día 1 y el 10, obtener el primer día del mes anterior
        if primer_dia_mes_actual.month == 1:
            # Caso especial: si es enero, el mes anterior es diciembre del año anterior
            primer_dia_mes_anterior = primer_dia_mes_actual.replace(
                year=primer_dia_mes_actual.year - 1, month=12)
        else:
            primer_dia_mes_anterior = primer_dia_mes_actual.replace(
                month=primer_dia_mes_actual.month - 1)
        fecha_limite = primer_dia_mes_anterior
    else:
        # Si está después del día 10, se usa el primer día del mes actual
        fecha_limite = primer_dia_mes_actual

    cond_activo = data[STATUS].str.lower() == 'activo'
    cond_inactivo_reciente = (
        (data[STATUS].str.lower() == 'inactivo') &
        (pd.to_datetime(data[TERMINATION_DATE],
         errors='coerce') >= fecha_limite)
    )

    data = data[cond_activo | cond_inactivo_reciente]
    # Filtrar solo workers con team requerida
    team_values = list(COLUMNS_TEAM.keys())
    data = data[data[TEAM].isin(team_values)]

    # Renombrar Team
    data[TEAM] = data[TEAM].replace(COLUMNS_TEAM)

    # Filtrar los datos primero
    data = data[(data[CAMPAIGN] == 'GLOVO') | (
        (data[CAMPAIGN] == 'GERENCIA 1') & (data[ROLE] == 'GERENTE DE OPERACIONES'))]

    # Verificar si los apellidos están presentes en 'employee_name', si es así, evitar duplicarlos
    data[NAME] = data.apply(
        lambda row: " ".join(
            dict.fromkeys(
                f"{row[EMPLOYEE_NAME]} {row[FATHER_LAST_NAME]} {row[MOTHER_LAST_NAME]}".split()
            )
        ),
        axis=1
    )

    # Normalizar los nombres
    data[NAME] = data[NAME].str.title()
    data[MANAGER] = data[MANAGER].str.title().str.strip()
    data[SUPERVISOR] = data[SUPERVISOR].str.title().str.strip()
    data[COORDINATOR] = data[COORDINATOR].str.title().str.strip()
    
    # Corregir el orden de los nombres en las columnas 'manager', 'supervisor' y 'coordinator'
    data = update_column_based_on_worker(data, data, MANAGER, NAME)
    data = update_column_based_on_worker(data, data, SUPERVISOR, NAME)
    data = update_column_based_on_worker(data, data, COORDINATOR, NAME)
    print('xd')
    print(data[data[DOCUMENT] == '77209106'])
    print(data[data[DOCUMENT] == '72012282'])
    # Convertir las fechas a formato datetime
    data[START_DATE] = pd.to_datetime(data[START_DATE], errors='coerce')
    data[TERMINATION_DATE] = pd.to_datetime(
        data[TERMINATION_DATE], errors='coerce')

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
    data[TRAINEE] = data[TENURE].apply(
        lambda x: "DESPEGANDO" if pd.notnull(x) and x < 1 else None)

    # *Formateamos los documentos para quitar los 0 iniciales, y tener concordancia con los otros archivos
    data[DOCUMENT] = data[DOCUMENT].astype(str).str.lstrip("0").astype(int)

    data[ROLE] = data[ROLE].replace({
        "RESPONSABLE DE OPERACIONES": "COORDINATOR",
        "EJECUTIVO DE CALIDAD": "QUALITY",
        "COORDINADOR DE GESTION EN TIEMPO REAL": "SR RTA",
        "JEFE DE OPERACIONES": "COORDINATOR",
        "ANALISTA DE GESTION EN TIEMPO REAL": "RTA",
        "RESPONSABLE DE CONTROL DE GESTION": "COORDINATOR RTA",
        "ANALISTA PPP": "PPP",
        "FORMADOR": "TRAINING",
        "AGENTE": "AGENT",
        "AGENTE 1": "QA/TR",
        "COORDINADOR DE CAPACITACION": "COORDINATOR TRAINING",
        "COORDINADOR DE CALIDAD": "COORDINATOR QUALITY",
        "SUPERVISOR DE CALIDAD": "SR QUALITY",
        "SUPERVISOR": "SUPERVISOR",
        "SUPERVISOR DE CAPACITACION": "SR TRAINING",
        "GERENTE DE OPERACIONES": "MANAGER"
    })

    # Opcional: Eliminar las columnas que no necesitas para mantener solo las que quieres almacenar
    columns_to_keep = [DOCUMENT, NAME, ROLE, STATUS, CAMPAIGN, TEAM, MANAGER,
                       SUPERVISOR, COORDINATOR, CONTRACT_TYPE, START_DATE, TERMINATION_DATE, WORK_TYPE, REQUIREMENT_ID, TENURE, TRAINEE]
    data = data[columns_to_keep]
    print('xd')
    print(data[data[DOCUMENT] == '77209106'])
    print(data[data[DOCUMENT] == '72012282'])
    return data
