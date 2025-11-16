import pandas as pd
from datetime import datetime

# 🔄 Mapeo de nombres de cola → equipos
QUEUE_MAPPING = {
    "VS-call-default": "Vendor Tier1",
    "VS-case-inbox-spa-ES-tier2": "Vendor Tier2",
    "RS-chat-spa-ES-tier1": "Rider Tier1",
    "RS-case-inbox-spa-ES-tier2": "Rider Tier2",
    "CS-chat-spa-ES-tier1": "Customer Tier1",
    "CS-case-inbox-spa-ES-tier2": "Customer Tier2",
}

def split_timestamp(value: str):
    """
    Convierte un string como 'November 11, 2025, 1:00 PM' en dos valores:
    date = '2025-11-11', interval = '13:00'
    """
    try:
        dt = datetime.strptime(value, "%B %d, %Y, %I:%M %p")
        return pd.Series([dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M")])
    except Exception:
        return pd.Series([None, None])

def clean_real_time_data(data: pd.DataFrame) -> pd.DataFrame:
    """
    Limpia y transforma el dataset proveniente del Excel de colas.
    """
    # 1️⃣ Filtrar solo las colas relevantes
    data = data[data["queue_name"].isin(QUEUE_MAPPING.keys())].copy()

    # 2️⃣ Reemplazar nombres de colas por nombres legibles
    data["queue_name"] = data["queue_name"].replace(QUEUE_MAPPING)

    # 3️⃣ Separar timestamp en columnas 'date' y 'interval'
    data[["date", "interval"]] = data["creation_timestamp_local"].apply(split_timestamp)

    # 4️⃣ Eliminar filas donde date o interval estén vacíos (como los totales)
    data = data.dropna(subset=["date", "interval"])

    # 5️⃣ Renombrar columnas a nombres estándar
    data = data.rename(
        columns={
            "queue_name": "team",
            "Incoming Contacts": "contacts_received",
            "AVG Resolution Time": "THT",
        }
    )

    data["contacts_received"] = (
        data["contacts_received"]
        .str.replace(",", "", regex=False)  # Eliminar comas
        .astype(float)  # Convertir a tipo float
    )

    # 6️⃣ Limpieza de SLA (% → float)
    if "SLA FRT" in data.columns:
        data["SLA FRT"] = (
            data["SLA FRT"]
            .astype(str)
            .str.replace("%", "", regex=False)
            .str.strip()
        )
        data["SLA FRT"] = pd.to_numeric(data["SLA FRT"], errors="coerce") / 100.0

    # 7️⃣ Eliminar columnas innecesarias
    if "pivot-grouping" in data.columns:
        data = data.drop(columns=["pivot-grouping"])

    # 8️⃣ Reordenar columnas finales
    cols = ["team", "date", "interval", "contacts_received", "SLA FRT", "THT"]
    data = data[[c for c in cols if c in data.columns]]

    # 9️⃣ Reiniciar índices
    data.reset_index(drop=True, inplace=True)

    return data