import pandas as pd
from datetime import date, datetime, timedelta

def clean_attendance(data: pd.DataFrame, target_date: pd.Timestamp | None = None) -> pd.DataFrame:
    # Si no se pasa target_date, utilizar el día actual
    if target_date is None:
        target_date = pd.Timestamp.today().normalize()  # Normaliza la fecha a medianoche

    print(target_date)
    # Asegurarse de que target_date es un tipo Timestamp
    if isinstance(target_date, date):
        target_date = pd.Timestamp(target_date).date()

    # Renombrar columnas
    data = data.rename(columns={
        'Agent Email': 'api_email',
    })

    data = data[['api_email',"Start Time", "End Time", "State"]]
    print('xd')
    print(target_date)
    print(data)
    # Convertir las columnas "Start Time" y "End Time" a datetime y manejar errores
    data["Start Time"] = pd.to_datetime(data["Start Time"], dayfirst=True, errors="coerce")
    print(data["Start Time"])
    data["End Time"] = pd.to_datetime(data["End Time"], dayfirst=True, errors="coerce")

    # Eliminar filas con "Data Unavailable"
    data = data[data["State"].str.upper() != "DATA UNAVAILABLE"]

    # Asegurarse de que las fechas estén en el formato correcto
    # data["Start Time"] = data["Start Time"].dt.normalize()  # Eliminar la hora, solo mantener la fecha

    # data["End Time"] = data["End Time"].dt.normalize()  # Eliminar la hora, solo mantener la fecha

    # Filtrar registros que correspondan a la fecha target (comparar solo la fecha, sin la hora)
    data = data[data["Start Time"].dt.date == target_date]
    print(data.head(30))
    # Lista para almacenar los resultados
    results = []

    # Agrupar por agente
    for agent, group in data.groupby("api_email"):
        # Imprimir el correo de cada agente, específicamente para "eddy.ayalaperez@providers.glovoapp.com"
        if agent == "aalespinozab.whl@service.glovoapp.com":
            print(f"Procesando registros para el correo: {agent}")
            print(group)

        # Filtrar registros con estado OFFLINE y los de Check-in
        offline = group[group["State"].str.upper() == "OFFLINE"]
        check_in_group = group[group["State"].str.upper() != "OFFLINE"]

        # Listas para los tiempos de check-in y check-out
        check_in_times = []
        check_out_times = []

        # Calcular duración en minutos para los tiempos de check-in
        for _, row in check_in_group.iterrows():
            duration = (row["End Time"] - row["Start Time"]).total_seconds() / 60  # Duración en minutos
            check_in_times.append([row["Start Time"].time(), duration])

        # Calcular duración en minutos para los tiempos de check-out (OFFLINE)
        for _, row in offline.iterrows():
            duration = (row["End Time"] - row["Start Time"]).total_seconds() / 60  # Duración en minutos
            check_out_times.append([row["Start Time"].time(), duration])

        # Si no hay registros de check-in o check-out, continuar con el siguiente agente
        if not check_in_times and not check_out_times:
            continue

        # Agregar los resultados de este agente
        result = {
            "api_email": agent,
            "date": target_date,
            "check_in_times": check_in_times,
            "check_out_times": check_out_times
        }

        # Imprimir los valores que se van a enviar al servicio
        if agent == "aalespinozab.whl@service.glovoapp.com":
            print(f"Valores de salida para el correo {agent}:")
            print(result)

        results.append(result)
    
    # Convertir los resultados en un DataFrame y devolverlo
    result_df = pd.DataFrame(results)
    
    return result_df
