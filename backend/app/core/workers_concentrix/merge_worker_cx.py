import pandas as pd
from app.core.utils.workers_cx.utils import fuzzy_match
from app.core.workers_concentrix.clean_people_consultation import clean_people_consultation
from app.core.workers_concentrix.clean_scheduling_ppp import clean_scheduling_ppp
from app.core.workers_concentrix.clean_report_kustomer import clean_report_kustomer
from app.core.utils.workers_cx.columns_names import NAME, KUSTOMER_NAME, KUSTOMER_EMAIL
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

    # Creamos la columna final 'team': si team_scheduling existe, lo usamos;
    # si no, tomamos team_people
    merged['team'] = merged['team_scheduling'].fillna(merged['team_people'])

    # Eliminamos las columnas intermedias
    merged = merged.drop(columns=['team_people', 'team_scheduling'])

    return merged



def merge_by_similar_name(df1: pd.DataFrame, df2: pd.DataFrame, column1: str, column2: str, threshold=95, fallback_threshold=75):
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
    Une df_final_worker con el DataFrame de 'despegando'.
    Empareja df_final_worker.KUSTOMER_EMAIL con df_despegando.usuario.
    Si no hay coincidencia, las columnas de despegando quedan vacías.
    Si requirement_id ya existe en df_final_worker, se reemplaza por el de df_despegando cuando exista.
    """
    # Normalizamos nombres de columnas en despegando
    df_despegando = df_despegando.rename(columns={c: str(c).strip().lower() for c in df_despegando.columns})
    df_despegando = df_despegando.rename(columns={
        "usuario": KUSTOMER_EMAIL,
        "qa_encargado": "qa_in_charge",
        "requirement_id": "requirement_id_despegando",  # evitamos choque en el merge
    })

    # merge
    merged = pd.merge(
        df_final_worker,
        df_despegando,
        on=KUSTOMER_EMAIL,
        how="left"
    )

    # Si requirement_id existe en df_final_worker, lo sobreescribimos con el de despegando (cuando no es nulo)
    if "requirement_id" in merged.columns and "requirement_id_despegando" in merged.columns:
        merged["requirement_id"] = merged["requirement_id_despegando"].combine_first(merged["requirement_id"])
        merged = merged.drop(columns=["requirement_id_despegando"])

    return merged

def generate_worker_cx_table(people_active: pd.DataFrame, people_inactive: pd.DataFrame, scheduling_ppp: pd.DataFrame, report_kustomer: pd.DataFrame, despegando: pd.DataFrame) -> pd.DataFrame:

    df_people_consultation = clean_people_consultation(people_active, people_inactive)
    df_scheduling_ppp = clean_scheduling_ppp(scheduling_ppp)
    df_report_kustomer = clean_report_kustomer(report_kustomer)

    df_people_and_ppp = merge_worker_data(df_people_consultation, df_scheduling_ppp)

    df_people_and_ppp['team'] = df_people_and_ppp['team'].replace({
       'CHAT USER' : 'CHAT CUSTOMER',
       'MAIL USER' : 'MAIL CUSTOMER',
       'CHAT GLOVER' : 'CHAT RIDER',
       'MAIL GLOVER': 'MAIL RIDER',
       'PARTNER CALL': 'CALL VENDOR',
       'MAIL PARTNER': 'MAIL VENDOR',
       'MIGRADOS CUSTOMER' : 'CHAT CUSTOMER HC',
       'MIGRADOS RIDER' : 'CHAT RIDER HC',
       'MIGRADOS VENDOR' : 'CALL VENDOR HC',
       'RUBIK CUSTOMER' : 'RUBIK CUSTOMER',
       'RUBIK RIDER' : 'RUBIK RIDER',
       'RUBIK VENDOR' : 'RUBIK VENDOR',
    })

    df_final_worker = merge_by_similar_name(df_people_and_ppp, df_report_kustomer, NAME, KUSTOMER_NAME)

    if despegando is not None:
        df_final_worker = merge_with_despegando(df_final_worker, despegando)

    return df_final_worker