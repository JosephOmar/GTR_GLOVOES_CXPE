import pandas as pd
import re
from datetime import datetime
import pytz
from rapidfuzz import process, fuzz
from typing import Union
import numpy as np

# utils.py

def normalize_name_sorted(name: str) -> str:
    # Convertir el nombre a minúsculas, separar en partes y ordenarlas (orden alfabetico para comparación)
    name_parts = name.strip().lower().split()
    name_parts.sort()
    
    # Unir las partes en el formato correcto (Título capitalizado)
    return " ".join(name_parts).title()

# Función para actualizar los nombres en una columna basándose en la columna 'WORKER'
def update_column_based_on_worker(dataframe: pd.DataFrame, dataframe2: pd.DataFrame, column_to_update: str, worker_column: str) -> pd.DataFrame:
    mapping_dict = {}

    # normalizar a minúsculas
    worker_values = dataframe2[worker_column].dropna().unique()
    worker_lower = [w.lower() for w in worker_values]

    for value in dataframe[column_to_update].dropna().unique():
        value_lower = value.lower()

        result = process.extractOne(
            value_lower,
            worker_lower,
            scorer=fuzz.token_sort_ratio  # token_sort_ratio ayuda a comparar nombres con distinto orden
        )

        if result:
            best_match_lower, score, _ = result

            if score > 90:
                # recuperar el valor original (no en minúscula) para reemplazar
                original = worker_values[worker_lower.index(best_match_lower)]
                mapping_dict[value] = original

    dataframe[column_to_update] = dataframe[column_to_update].replace(mapping_dict)
    return dataframe

# ? Limpiar observaciones (NA transformar a vacios)
def clean_observations(valor):
    if pd.isna(valor) or valor in [
        0,
        "0",
        "nan",
        None,
    ]:  # * Verificar valores vacíos o equivalentes
        return ""
    return valor

# Función para limpiar y capitalizar el nombre
def clean_worker_name(name: str) -> str:
    # Verificar si el valor es NaN y convertirlo en cadena vacía
    if isinstance(name, float) and np.isnan(name):
        return ""  # Retornar una cadena vacía si es NaN

    # Verificar que el valor sea una cadena antes de proceder
    if not isinstance(name, str):
        return ""  # Retornar una cadena vacía si no es una cadena

    # 1. Eliminar cualquier texto después del paréntesis (por ejemplo, '(DYLAT)')
    name = re.sub(r'\(.*\)', '', name).strip()

    # 2. Separar las partes del nombre en caso de que estén en camelCase
    # name = re.sub(r'(?<=[a-z])(?=[A-Z])', ' ', name)

    # 3. Separar por espacios y capitalizar cada parte
    name_parts = name.split()

    # 4. Capitalizar cada parte del nombre
    name = ' '.join([part.capitalize() for part in name_parts])

    return name

# Función para convertir milisegundos en horas (con una decimal)
def convert_ms_to_hours(ms: int) -> float:
    hours = ms / (1000 * 60 * 60)  # Convertir milisegundos a horas
    return round(hours, 1)  # Redondear a 1 decimal

# Función para convertir la fecha a formato 'hora' y 'fecha'
def convert_login_time_to_local_timezone(login_time: Union[str, float, None]) -> tuple:
    if not isinstance(login_time, str) or not login_time:
        return None, None  # Valor por defecto si está vacío o no es string

    try:
        dt = datetime.fromisoformat(login_time.replace("Z", "+00:00"))
        peru_tz = pytz.timezone('America/Lima')
        dt_peru = dt.astimezone(peru_tz)
        recent_login = dt_peru.strftime("%H:%M:%S")
        date = dt_peru.strftime("%Y-%m-%d")
        return recent_login, date
    except Exception:
        return None, None


# Función para encontrar el nombre más similar utilizando rapidfuzz
def fuzzy_match(name, name_list, threshold):
    if not isinstance(name, str) or not name.strip():
        return None

    result = process.extractOne(
        name,
        name_list,
        scorer=fuzz.token_set_ratio  # Usa una métrica más adecuada para nombres
    )
    if result:
        best_match, score, _ = result
        if score >= threshold:
            return best_match
    return None