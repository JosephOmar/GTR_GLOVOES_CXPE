import pandas as pd
from datetime import time

def clean_attendance(data: pd.DataFrame, target_date: pd.Timestamp | None = None) -> pd.DataFrame:
    # Renombrar columnas
    data = data.rename(columns={
        'Agent Email' : 'kustomer_email',
    })
    data["Start Time"] = pd.to_datetime(data["Start Time"], dayfirst=True, errors="coerce")

    # Si no se pasa fecha → usar la actual
    if target_date is None:
        target_date = pd.Timestamp.today().normalize()  # día actual a medianoche

    results = []

    data["State"] = data["State"].fillna("")

    # Agrupar por agente
    for agent, group in data.groupby("kustomer_email"):
        # Filtrar ONLINE y OFFLINE solo del día elegido
        group = group[group["Start Time"].dt.normalize() == target_date]

        if group.empty:
            continue

        # Filtrar ONLINE y AVAILABLE
        online = group[
            (group["State"].str.upper() == "ONLINE") |
            (group["State"].str.upper().str.contains("AVAILABLE", regex=False))
        ]
        offline = group[group["State"].str.upper() == "OFFLINE"]

        if online.empty:
            # Nunca se conectó → marcar sin asistencia
            continue

        # Check-in = primer ONLINE
        check_in = online["Start Time"].min()

        # Buscar OFFLINE posterior al check-in
        valid_offline = offline[offline["Start Time"] > check_in]
        check_out = valid_offline["Start Time"].max() if not valid_offline.empty else None

        # Convertir a tipos compatibles con SQLAlchemy
        check_in_time = check_in.to_pydatetime().time() if pd.notna(check_in) else None
        check_out_time = check_out.to_pydatetime().time() if pd.notna(check_out) else None

        results.append({
            "kustomer_email": agent,
            "date": check_in.date(),     # guardamos solo la fecha
            "check_in": check_in_time,   # guardamos la hora
            "check_out": check_out_time  # guardamos la hora
        })

    return pd.DataFrame(results)
