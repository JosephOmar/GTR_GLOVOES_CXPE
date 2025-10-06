import pandas as pd
from datetime import time

def clean_attendance(data: pd.DataFrame, target_date: pd.Timestamp | None = None) -> pd.DataFrame:
    # Renombrar columnas
    data = data.rename(columns={
        'Agent Email': 'kustomer_email',
    })
    data["Start Time"] = pd.to_datetime(data["Start Time"], dayfirst=True, errors="coerce")

    # Si no se pasa fecha → usar la actual
    if target_date is None:
        target_date = pd.Timestamp.today().normalize()

    results = []
    data["State"] = data["State"].fillna("")

    # Agrupar por agente
    for agent, group in data.groupby("kustomer_email"):
        # Filtrar registros del día específico
        group = group[group["Start Time"].dt.normalize() == target_date]
        if group.empty:
            continue

        # Filtrar ONLINE/ASSIGNED TASK/AVAILABLE
        online = group[
            (group["State"].str.upper() == "ONLINE") |
            (group["State"].str.upper() == "ASSIGNED TASK") |
            (group["State"].str.upper().str.contains("AVAILABLE", regex=False))
        ]
        # Filtrar OFFLINE
        offline = group[group["State"].str.upper() == "OFFLINE"]

        # Convertir a listas de tiempos
        check_in_times = online["Start Time"].dt.time.tolist()
        check_out_times = offline["Start Time"].dt.time.tolist()

        if not check_in_times:
            # Nunca se conectó
            continue

        results.append({
            "kustomer_email": agent,
            "date": target_date.date(),
            "check_in_times": check_in_times,
            "check_out_times": check_out_times
        })

    return pd.DataFrame(results)