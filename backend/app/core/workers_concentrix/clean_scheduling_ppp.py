import pandas as pd
from app.core.utils.workers_cx.utils import clean_observations
from app.core.utils.workers_cx.columns_names import DOCUMENT, OBSERVATION_1, OBSERVATION_2

COLUMNS_SCHEDULING_PPP = {
    "DNI": DOCUMENT,
    "Observaciones 1°": OBSERVATION_1,
    "Observaciones 2°": OBSERVATION_2
}

REQUIRED_COLUMNS = list(COLUMNS_SCHEDULING_PPP.keys())

def clean_scheduling_ppp(data: pd.DataFrame) -> pd.DataFrame:
    # 🔍 Limpieza inicial de nombres de columnas
    data.columns = data.columns.str.strip()

    # ✅ Validar que las columnas requeridas están presentes
    missing = [col for col in REQUIRED_COLUMNS if col not in data.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas en el archivo: {missing}. Columnas encontradas: {data.columns.tolist()}")

    # 🧼 Renombrar columnas
    data = data.rename(columns=COLUMNS_SCHEDULING_PPP)

    # 📄 Limpiar documento
    data[DOCUMENT] = pd.to_numeric(data[DOCUMENT], errors="coerce").dropna().astype(int)

    # 💬 Limpiar observaciones
    data[OBSERVATION_1] = data[OBSERVATION_1].astype(str).str.strip().apply(clean_observations) 
    data[OBSERVATION_2] = data[OBSERVATION_2].astype(str).str.strip().apply(clean_observations)

    # 🧽 Mantener solo las columnas necesarias
    data = data[[DOCUMENT, OBSERVATION_1, OBSERVATION_2]]

    return data
