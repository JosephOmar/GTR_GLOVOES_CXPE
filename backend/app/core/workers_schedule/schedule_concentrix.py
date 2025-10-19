import pandas as pd
import numpy as np
from datetime import date, timedelta, time
from app.core.utils.workers_cx.columns_names import (
    DOCUMENT, DATE, START_TIME, END_TIME,
    DAY, BREAK_START, BREAK_END, REST_DAY
)


def schedule_concentrix(
    data: pd.DataFrame,
    data_obs: pd.DataFrame,
    week: int | None = None,
    year: int | None = None
) -> pd.DataFrame:
    """Genera el DataFrame normalizado de horarios para Concentrix, con limpieza completa de NaT y compatibilidad con SQLite/Postgres."""

    # --- 🔹 Normalización de documentos ---
    data["NRO_DOCUMENTO"] = data["NRO_DOCUMENTO"].astype(str).str.lstrip("0")
    data_obs["NRO_DOCUMENTO"] = data_obs["NRO_DOCUMENTO"].astype(str).str.lstrip("0")

    # --- 🔹 Determinar semana y fechas ---
    today = date.today()
    year = year or today.year
    week = week or today.isocalendar()[1]
    monday = date.fromisocalendar(year, week, 1)

    days = ["Lunes", "Martes", "Miercoles", "Jueves", "Viernes", "Sabado", "Domingo"]
    idx_map = {d: i for i, d in enumerate(days)}

    # --- 🔹 Generar registros vectorizados ---
    records = []

    for day in days:
        idx = idx_map[day]
        current_date = monday + timedelta(days=idx)

        ing_col = f"INGRESO_{day.upper()}"
        sal_col = f"SALIDA_{day.upper()}"
        ref_col = f"REFRIGERIO_{day.upper()}"

        sub = data[["NRO_DOCUMENTO", ing_col, sal_col, ref_col]].copy()
        sub.rename(columns={
            "NRO_DOCUMENTO": DOCUMENT,
            ing_col: START_TIME,
            sal_col: END_TIME,
            ref_col: "REF"
        }, inplace=True)

        sub[DAY] = day
        sub[DATE] = current_date

        # --- 🔹 Detectar día de descanso ---
        sub[REST_DAY] = sub[START_TIME].isna() | (
            sub[START_TIME].astype(str).str.strip().str.upper() == "DSO"
        )

        # --- 🔹 Separar refrigerio en inicio y fin ---
        ref_split = sub["REF"].astype(str).str.split("-", n=1, expand=True)
        sub[BREAK_START] = ref_split[0].str.strip()
        sub[BREAK_END] = ref_split[1].str.strip() if ref_split.shape[1] > 1 else None
        sub.drop(columns=["REF"], inplace=True)

        # --- 🔹 Observaciones (vacaciones, faltas, etc.) ---
        date_col = current_date.strftime("%d/%m/%Y")
        if date_col in data_obs.columns:
            obs_map = data_obs.set_index("NRO_DOCUMENTO")[date_col]
            sub["obs"] = sub[DOCUMENT].map(obs_map)
            # eliminar observaciones que son horas
            sub["obs"] = sub["obs"].where(
                ~sub["obs"].astype(str).str.match(r"^\d{1,2}:\d{2}(:\d{2})?$")
            )
        else:
            sub["obs"] = None

        records.append(sub)

    # --- 🔹 Concatenar todo ---
    df = pd.concat(records, ignore_index=True)

    # --- 🔹 Convertir horas correctamente ---
    for col in (START_TIME, END_TIME, BREAK_START, BREAK_END):
        df[col] = pd.to_datetime(df[col], format="%H:%M:%S", errors="coerce").dt.time

    # --- 🔹 Limpiar valores NaT / NaN (fundamental para SQLite) ---
    for col in (START_TIME, END_TIME, BREAK_START, BREAK_END):
        df[col] = df[col].apply(lambda x: x if isinstance(x, time) else None)

    df["obs"] = df["obs"].replace({pd.NA: None, np.nan: None})

    # --- 🔹 Retornar DataFrame listo ---
    return df[[DOCUMENT, DATE, DAY, START_TIME, END_TIME, BREAK_START, BREAK_END, REST_DAY, "obs"]]
