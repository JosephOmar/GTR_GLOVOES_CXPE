import pandas as pd
from app.core.utils.workers_cx.utils import fuzzy_match
from app.core.workers_concentrix.clean_people_consultation import clean_people_consultation
from app.core.workers_concentrix.clean_scheduling_ppp import clean_scheduling_ppp
from app.core.utils.workers_cx.utils import update_column_based_on_worker
from app.core.utils.workers_cx.columns_names import NAME, API_EMAIL, API_ID, DOCUMENT, SUPERVISOR, REQUIREMENT_ID, TEAM
import numpy as np

# Función para combinar los DataFrames basados en la columna 'DOCUMENT'
def merge_worker_data(df_people_consultation: pd.DataFrame,
                      df_scheduling_ppp: pd.DataFrame) -> pd.DataFrame:
    """
    Combina df_people_consultation y df_scheduling_ppp sobre 'document',
    rellenando observaciones y eligiendo TEAM de df_scheduling_ppp
    por encima del de df_people_consultation cuando exista.
    """
    # Usamos suffixes para distinguir claramente las dos columnas 'team'
    merged = pd.merge(
        df_people_consultation,
        df_scheduling_ppp,
        on='document',
        how='left',
        suffixes=('_people', '_scheduling')
    ) 

    # Rellenar observaciones vacías
    merged['observation_1'] = merged['observation_1'].fillna('')
    merged['observation_2'] = merged['observation_2'].fillna('')

    # Convertimos cadenas vacías en NaN para que combine correctamente
    merged['team_scheduling'] = merged['team_scheduling'].replace('', np.nan)
    merged['supervisor_scheduling'] = merged['supervisor_scheduling'].replace('', np.nan)
    merged['requirement_id_scheduling'] = merged['requirement_id_scheduling'].replace('', np.nan)

    # Creamos la columna final 'team': si team_scheduling existe, lo usamos;
    # si no, tomamos team_people
    merged[TEAM] = merged['team_scheduling'].fillna(merged['team_people'])
    merged[SUPERVISOR] = merged['supervisor_scheduling'].fillna(merged['supervisor_people'])
    merged[REQUIREMENT_ID] = merged['requirement_id_scheduling'].fillna(merged['requirement_id_people'])

    # Eliminamos las columnas intermedias
    merged = merged.drop(columns=['team_people', 'team_scheduling','supervisor_people', 'supervisor_scheduling','requirement_id_people', 'requirement_id_scheduling'])

    return merged

def merge_by_similar_name(df1: pd.DataFrame, df2: pd.DataFrame, column1: str, column2: str, threshold=95, fallback_threshold=60):
    """
    Realiza un merge entre dos DataFrames usando fuzzy matching en los nombres utilizando rapidfuzz.
    """
    # Crear una lista de los nombres de la segunda columna (en este caso `kustomer_name`)
    name_list_df2 = df2[column2].tolist()
    
    # Función para encontrar el nombre más similar usando rapidfuzz
    def get_best_match(name):
        # Intentar con el umbral normal
        best_match = fuzzy_match(name, name_list_df2, threshold)
        
        # Si no se encuentra una coincidencia, intentar con el umbral más bajo
        if not best_match:
            best_match = fuzzy_match(name, name_list_df2, fallback_threshold)
        
        return best_match
    
    # Aplicar la función a la primera columna
    df1["best_match"] = df1[column1].apply(get_best_match)
    
    # Hacer el merge entre los DataFrames utilizando las mejores coincidencias
    merged_df = pd.merge(df1, df2, left_on="best_match", right_on=column2, how="left")
    
    # Eliminar la columna temporal "best_match"
    merged_df = merged_df.drop(columns=["best_match"])
    
    return merged_df

def merge_with_despegando(df_final_worker: pd.DataFrame, df_despegando: pd.DataFrame) -> pd.DataFrame:
    """
    Une df_final_worker con el DataFrame de 'despegando' solo para filas con trainee == 'DESPEGANDO'.
    Empareja df_final_worker.DOCUMENT con df_despegando.document.
    Si no hay coincidencia, las columnas de despegando quedan vacías.
    requirement_id y supervisor se reemplazan por los valores de despegando cuando existen.
    """

    # Normalizamos nombres de columnas en despegando
    df_despegando = df_despegando.rename(columns={c: str(c).strip().lower() for c in df_despegando.columns})
    df_despegando = df_despegando.rename(columns={
        "document": DOCUMENT,
        "supervisor": "supervisor_despegando",
        "requirement_id": "requirement_id_despegando",
    })

    # # Dividimos en dos: los que son DESPEGANDO y los demás
    # df_despegando_mask = df_final_worker["trainee"].str.upper() == "DESPEGANDO"
    # df_despegando_part = df_final_worker[df_despegando_mask].copy()
    # df_normal_part = df_final_worker[~df_despegando_mask].copy()

    # Merge solo para los DESPEGANDO
    merged = pd.merge(
        df_final_worker,
        df_despegando,
        on=DOCUMENT,
        how="left"
    )

    # Reemplazo de supervisor y requirement_id si existen valores en despegando
    if "requirement_id_despegando" in merged.columns:
        merged["requirement_id"] = merged["requirement_id_despegando"].combine_first(merged["requirement_id"])
        merged = merged.drop(columns=["requirement_id_despegando"])

    if "supervisor_despegando" in merged.columns:
        merged["supervisor"] = merged["supervisor_despegando"].combine_first(merged["supervisor"])
        merged = merged.drop(columns=["supervisor_despegando"])

    # Reconstruimos todo el dataframe
    # result = pd.concat([merged, df_normal_part], ignore_index=True)

    return merged

def generate_worker_cx_table(
    people_active: pd.DataFrame,
    people_inactive: pd.DataFrame,
    scheduling_ppp: pd.DataFrame,
    api_id: pd.DataFrame
) -> pd.DataFrame:
    """Genera la tabla final de trabajadores de Concentrix (optimizada y con control de tipos)."""

    # --- Limpieza y normalización inicial
    df_people_consultation = clean_people_consultation(people_active, people_inactive)
    df_scheduling_ppp = clean_scheduling_ppp(scheduling_ppp)

    # --- Normalización de columnas en api_id (ANTES de convertir tipos)
    api_id = api_id.rename(columns={
        'DOCUMENT': DOCUMENT,
        'API EMAIL': API_EMAIL,
        'API ID': API_ID
    })
    api_id = api_id[[DOCUMENT, API_EMAIL, API_ID]]

    # --- Asegurar tipo string en todas las fuentes (AHORA sí funciona)
    for df in [df_people_consultation, df_scheduling_ppp, api_id]:
        if DOCUMENT in df.columns:
            df[DOCUMENT] = (
                df[DOCUMENT]
                .astype(str)
                .str.replace(r"\.0$", "", regex=True)
                .str.strip()
            )

    # --- Merge 1: people + scheduling
    df_people_and_ppp = merge_worker_data(df_people_consultation, df_scheduling_ppp)

    # --- Ajustar TEAM
    df_people_and_ppp[TEAM] = df_people_and_ppp[TEAM].replace({
        'CUSTOMER TIER1': 'CUSTOMER TIER1',
        'RIDER TIER1': 'RIDER TIER1',
        'VENDOR TIER1': 'VENDOR CALL',
        'CUSTOMER TIER2': 'CUSTOMER TIER2',
        'RIDER TIER2': 'RIDER TIER2',
        'VENDOR TIER2': 'VENDOR MAIL',
        'MIGRADOS VENDOR': 'VENDOR TIER1',
        'RUBIK VENDOR': 'VENDOR TIER2',
    })

    # --- Merge 2: con api_id (ahora todos tienen document como str)
    df_final_worker = pd.merge(
        df_people_and_ppp,
        api_id,
        on=DOCUMENT,
        how="left"
    )

    # --- Actualizar supervisor con nombres limpios
    df_final_worker = update_column_based_on_worker(
        df_final_worker,
        df_people_consultation,
        SUPERVISOR,
        NAME
    )

    return df_final_worker
