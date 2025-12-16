import pandas as pd
from datetime import datetime

# 🔄 Mapeo de nombres de cola → equipos
QUEUE_MAPPING = {
    "VS-call-default": "Vendor Tier1",
    "VS-case-inbox-spa-ES-tier2": "Vendor Tier2",
    "RS-chat-spa-ES-tier1": "Rider Tier1",
    "RS-case-inbox-spa-ES-tier2": "Rider Tier2",
    "CS-case-inbox-spa-ES-tier2": "Customer Tier2",
    "CS-chat-spa-ES-live-order": "Customer Tier1",
    "CS-chat-spa-ES-nonlive-order": "Customer Tier1",
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
    # 1️⃣ Filtrar solo las colas relevantes
    data = data[data["queue_name"].isin(QUEUE_MAPPING.keys())].copy()

    # 2️⃣ Reemplazar los nombres de cola por nombres de equipos
    data["queue_name"] = data["queue_name"].replace(QUEUE_MAPPING)

    # 3️⃣ Dividir timestamp
    data[["date", "interval"]] = data["creation_timestamp_local"].apply(split_timestamp)

    # 4️⃣ Quitar filas sin fecha u hora
    data = data.dropna(subset=["date", "interval"])

    # 5️⃣ Renombrar columnas
    data = data.rename(
        columns={
            "queue_name": "team",
            "Incoming Contacts": "contacts_received",
            "AVG Resolution Time": "THT",
        }
    )

    # Convertir contacts_received a número
    data["contacts_received"] = (
        data["contacts_received"]
        .astype(str)
        .str.replace(",", "", regex=False)
    )
    data["contacts_received"] = pd.to_numeric(
        data["contacts_received"], errors="coerce"
    )

    # Convertir SLA FRT a decimal
    if "SLA FRT" in data.columns:
        data["SLA FRT"] = (
            data["SLA FRT"].astype(str).str.replace("%", "", regex=False).str.strip()
        )
        data["SLA FRT"] = pd.to_numeric(data["SLA FRT"], errors="coerce") / 100

    # Convertir THT a float
    if "THT" in data.columns:
        data["THT"] = pd.to_numeric(data["THT"], errors="coerce")

    # ------------------------------------------
    # 🔥 6️⃣ AGRUPACIÓN final con métricas correctas
    # ------------------------------------------

    # Para promedios ponderados
    def weighted_avg(series, weights):
        try:
            return (series * weights).sum() / weights.sum()
        except Exception:
            return None

    group_cols = ["team", "date", "interval"]

    grouped = data.groupby(group_cols).apply(
        lambda g: pd.Series({
            "contacts_received": g["contacts_received"].sum(),
            "SLA FRT": weighted_avg(g["SLA FRT"], g["contacts_received"]) if "SLA FRT" in g else None,
            "THT": weighted_avg(g["THT"], g["contacts_received"]) if "THT" in g else None,
        })
    ).reset_index()

    # 7️⃣ Reordenar columnas
    final_cols = ["team", "date", "interval", "contacts_received", "SLA FRT", "THT"]
    grouped = grouped[[c for c in final_cols if c in grouped.columns]]
    print(grouped[grouped['team'] == 'Customer Tier1'])
    return grouped
