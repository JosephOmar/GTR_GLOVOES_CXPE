import pandas as pd
from datetime import date

def clean_attendance(data: pd.DataFrame, target_date: pd.Timestamp | None = None) -> pd.DataFrame:
    # 1) Fecha objetivo (como date para comparar contra .dt.date)
    if target_date is None:
        target_date = pd.Timestamp.today().normalize()
    # Asegura que terminemos con un datetime.date para el filtro
    target_date_date = pd.Timestamp(target_date).date()

    # 2) Renombrar columnas y quedarnos con lo necesario
    data = data.rename(columns={'Agent Email': 'api_email'})
    data = data[['api_email', 'Start Time', 'End Time', 'State']].copy()

    # 3) Limpiar estado y filtrar "DATA UNAVAILABLE"
    data['State'] = data['State'].astype(str)  # por si hay NaN
    data = data[data['State'].str.upper() != 'DATA UNAVAILABLE']

    # 4) Convertir a datetime con formato explícito (ISO seguro)
    # Si tus strings siempre son "YYYY-mm-dd HH:MM:SS"
    data['Start Time'] = pd.to_datetime(
        data['Start Time'], format="%Y-%m-%d %H:%M:%S", errors='coerce'
    )
    data['End Time'] = pd.to_datetime(
        data['End Time'], format="%Y-%m-%d %H:%M:%S", errors='coerce'
    )

    # 5) Filtrar por la fecha objetivo (comparando solo la fecha)
    data = data[data['Start Time'].dt.date == target_date_date]

    # 6) Agrupar y construir resultado
    results = []
    for agent, group in data.groupby('api_email'):
        list_status = ['OFFLINE', 'UNAVAILABLE', 'BUSY']
        offline = group[group['State'].str.upper().isin(list_status)]
        check_in_group = group[~group['State'].str.upper().isin(list_status)]

        check_in_times = []
        check_out_times = []

        for _, row in check_in_group.iterrows():
            if pd.notna(row['Start Time']) and pd.notna(row['End Time']):
                duration = (row['End Time'] - row['Start Time']).total_seconds() / 60
                check_in_times.append([row['Start Time'].time(), duration])

        for _, row in offline.iterrows():
            if pd.notna(row['Start Time']) and pd.notna(row['End Time']):
                duration = (row['End Time'] - row['Start Time']).total_seconds() / 60
                check_out_times.append([row['Start Time'].time(), duration])

        if not check_in_times and not check_out_times:
            continue

        results.append({
            'api_email': agent,
            'date': target_date_date,
            'check_in_times': check_in_times,
            'check_out_times': check_out_times
        })

    return pd.DataFrame(results) 
