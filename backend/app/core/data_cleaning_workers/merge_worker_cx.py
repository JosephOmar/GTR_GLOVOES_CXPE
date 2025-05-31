import pandas as pd
from app.core.utils.workers_cx.utils import fuzzy_match
from app.core.data_cleaning_workers.clean_people_consultation import clean_people_consultation
from app.core.data_cleaning_workers.clean_scheduling_ppp import clean_scheduling_ppp
from app.core.data_cleaning_workers.clean_report_kustomer import clean_report_kustomer
from app.core.utils.workers_cx.columns_names import NAME, KUSTOMER_NAME

# Función para combinar los DataFrames basados en la columna 'DOCUMENT'
def merge_worker_data(df_people_consultation: pd.DataFrame, df_scheduling_ppp: pd.DataFrame) -> pd.DataFrame:
    # Realizar un merge (join) en la columna 'DOCUMENT'
    merged_data = pd.merge(df_people_consultation, df_scheduling_ppp, on='document', how='left')

    # Reemplazar los valores NaN de las columnas correspondientes a 'OBSERVATION_1' y 'OBSERVATION_2' por cadenas vacías
    merged_data['observation_1'] = merged_data['observation_1'].fillna('')
    merged_data['observation_2'] = merged_data['observation_2'].fillna('')

    return merged_data

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

def generate_worker_cx_table(people_consultation: pd.DataFrame, scheduling_ppp: pd.DataFrame, report_kustomer: pd.DataFrame) -> pd.DataFrame:

    df_people_consultation = clean_people_consultation(people_consultation)
    df_scheduling_ppp = clean_scheduling_ppp(scheduling_ppp)
    df_report_kustomer = clean_report_kustomer(report_kustomer)

    df_people_and_ppp = merge_worker_data(df_people_consultation, df_scheduling_ppp)
    df_final_worker = merge_by_similar_name(df_people_and_ppp, df_report_kustomer, NAME, KUSTOMER_NAME)

    return df_final_worker