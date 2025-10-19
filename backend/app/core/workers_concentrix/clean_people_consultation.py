import pandas as pd
import numpy as np
from datetime import datetime
from app.core.utils.workers_cx.utils import update_column_based_on_worker
from app.core.utils.workers_cx.columns_names import (
    DOCUMENT, ROLE, STATUS, CAMPAIGN, TEAM, MANAGER, SUPERVISOR, COORDINATOR,
    CONTRACT_TYPE, START_DATE, TERMINATION_DATE, WORK_TYPE, REQUIREMENT_ID,
    EMPLOYEE_NAME, FATHER_LAST_NAME, MOTHER_LAST_NAME, NAME, TENURE, TRAINEE
)
from app.core.utils.workers_cx.columns_names import (
    CHAT_CUSTOMER, CHAT_RIDER, MAIL_CUSTOMER, MAIL_RIDER, MAIL_VENDORS,
    CALL_VENDORS, GLOVO_SPAIN, GERENCIA, UPDATE
)

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

FILTER_MANAGEMENT = ['CAPACITACION', 'EXPERIENCIA CLIENTE', 'GERENCIA 1', 'GLOVO']

COLUMNS_TEAM = {
    "CHAT USER ESPAÑA": CHAT_CUSTOMER,
    "ID VERIFY + PAYMENT ES ": CHAT_CUSTOMER,
    "CHAT GLOVER ESPAÑA": CHAT_RIDER,
    "MAIL PARTNER": MAIL_VENDORS,
    "PARTNERCALL ESPAÑA": CALL_VENDORS,
    "GERENCIA 1": GERENCIA,
    "MAIL GLOVER ESPAÑA": MAIL_RIDER,
    "PARTNER ONLINECALLS": CALL_VENDORS,
    "INCIDENTES SEVEROS ESPAÑA": MAIL_CUSTOMER,
    "MAIL USER ESPAÑA": MAIL_CUSTOMER,
    "GLOVO": GLOVO_SPAIN,
    "GLOVO ESPAÑA": GLOVO_SPAIN,
    "UPDATE": UPDATE
}

def clean_people_consultation(data_active: pd.DataFrame, data_inactive: pd.DataFrame) -> pd.DataFrame:
    """Versión optimizada y estable de limpieza de trabajadores."""
    # --- 1️⃣ Unir solo DataFrames válidos (evita FutureWarning)
    frames = []
    for df in [data_active, data_inactive]:
        if df is not None and not df.empty:
            # eliminamos columnas totalmente vacías para evitar el warning
            df = df.dropna(axis=1, how="all")
            frames.append(df)

    if not frames:
        return pd.DataFrame()  # ambos vacíos

    data = pd.concat(frames, ignore_index=True)
    data = data.rename(columns=COLUMNS_PEOPLE_CONSULTATION)

    # --- 2️⃣ Filtrar activos/inactivos recientes
    current_date = datetime.now()
    first_day_month = current_date.replace(day=1)
    limit_date = (first_day_month - pd.DateOffset(months=1)) if current_date.day <= 10 else first_day_month

    data[TERMINATION_DATE] = pd.to_datetime(data[TERMINATION_DATE], errors='coerce')
    mask = (
        (data[STATUS].str.lower() == 'activo') |
        ((data[STATUS].str.lower() == 'inactivo') & (data[TERMINATION_DATE] >= limit_date))
    )
    data = data.loc[mask]

    # --- 3️⃣ Filtros de campaña y team
    data = data[data[CAMPAIGN].isin(FILTER_MANAGEMENT)]
    data[TEAM] = data[TEAM].replace(COLUMNS_TEAM)
    data = data.loc[
        (data[CAMPAIGN] == 'GLOVO') |
        ((data[CAMPAIGN] == 'GERENCIA 1') & (data[ROLE] == 'GERENTE DE OPERACIONES'))
    ]

    # --- 4️⃣ Construir nombres completos
    data[[EMPLOYEE_NAME, FATHER_LAST_NAME, MOTHER_LAST_NAME]] = data[
        [EMPLOYEE_NAME, FATHER_LAST_NAME, MOTHER_LAST_NAME]
    ].fillna('')

    full_names = (
        data[EMPLOYEE_NAME].str.strip() + ' ' +
        data[FATHER_LAST_NAME].str.strip() + ' ' +
        data[MOTHER_LAST_NAME].str.strip()
    ).str.split().apply(lambda x: " ".join(dict.fromkeys(x)))  # elimina duplicados
    data[NAME] = full_names.str.title()

    # --- 5️⃣ Normalizar jerárquicos
    for col in [MANAGER, SUPERVISOR, COORDINATOR]:
        data[col] = data[col].fillna('').str.title().str.strip()

    # --- 6️⃣ Fechas y antigüedad vectorizadas
    data[START_DATE] = pd.to_datetime(data[START_DATE], errors='coerce')
    now = pd.Timestamp.now()
    valid_mask = data[START_DATE].notna()
    diff_months = (now.year - data.loc[valid_mask, START_DATE].dt.year) * 12 + \
                  (now.month - data.loc[valid_mask, START_DATE].dt.month)
    data.loc[valid_mask, TENURE] = diff_months.clip(lower=0)
    data[TRAINEE] = np.where(data[TENURE] < 1, "DESPEGANDO", None)

    # --- 7️⃣ Documento sin ceros
    data[DOCUMENT] = data[DOCUMENT].astype(str).str.lstrip("0")

    # --- 8️⃣ Actualizar columnas jerárquicas con nombres reales (usa función optimizada)
    for col in [MANAGER, SUPERVISOR, COORDINATOR]:
        data = update_column_based_on_worker(data, data, col, NAME)

    # --- 9️⃣ Mapear roles
    role_map = {
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
    }
    data[ROLE] = data[ROLE].replace(role_map)

    # --- 🔟 Selección final
    cols = [
        DOCUMENT, NAME, ROLE, STATUS, CAMPAIGN, TEAM, MANAGER, SUPERVISOR, COORDINATOR,
        CONTRACT_TYPE, START_DATE, TERMINATION_DATE, WORK_TYPE, REQUIREMENT_ID, TENURE, TRAINEE
    ]
    data = data[cols].drop_duplicates(ignore_index=True)

    return data
