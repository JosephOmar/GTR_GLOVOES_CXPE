from app.core.workers_ubycall.clean_master_glovo import clean_master_glovo
from app.core.workers_ubycall.clean_scheduling_ubycall import clean_scheduling_ubycall
from app.core.workers_concentrix.clean_report_kustomer import clean_report_kustomer
from app.core.utils.workers_cx.columns_names import NAME, DOCUMENT, MANAGER, CAMPAIGN, ROLE, WORK_TYPE, CONTRACT_TYPE, TERMINATION_DATE, REQUIREMENT_ID, TRAINEE, OBSERVATION_1, OBSERVATION_2, KUSTOMER_EMAIL, KUSTOMER_ID, KUSTOMER_NAME
import pandas as pd
import numpy as np
from app.core.workers_concentrix.clean_report_kustomer import clean_report_kustomer
from app.core.workers_concentrix.merge_worker_cx import merge_by_similar_name

def generate_worker_uby_table(master_glovo: pd.DataFrame, scheduling_ubycall: pd.DataFrame, report_kustomer: pd.DataFrame, people_active: pd.DataFrame, people_inactive: pd.DataFrame) -> pd.DataFrame:
    # Concatenar ambos DataFrames
    master_glovo = clean_master_glovo(master_glovo, people_active, people_inactive)
    scheduling_ubycall = clean_scheduling_ubycall(scheduling_ubycall)
    report_kustomer = clean_report_kustomer(report_kustomer)

    combined_data = pd.concat([master_glovo, scheduling_ubycall])
    
    # Eliminar duplicados basados en 'DOCUMENT', manteniendo solo el primer registro
    # En este caso, se mantiene el primer registro de 'master_data' y elimina los duplicados de 'scheduling_data'
    combined_data = combined_data.drop_duplicates(subset=[DOCUMENT], keep='first')
    # Completar valores vacios
    combined_data[MANAGER] = 'Rosa Del Pilar Agurto Quispe'
    combined_data[CAMPAIGN] = 'GLOVO'
    combined_data[ROLE] = 'AGENT'
    combined_data[WORK_TYPE] = 'REMOTO'
    combined_data[CONTRACT_TYPE] = 'UBYCALL'
    combined_data[TERMINATION_DATE] = np.nan
    combined_data[REQUIREMENT_ID] = np.nan
    combined_data[TRAINEE] = np.nan
    combined_data[OBSERVATION_1] = np.nan
    combined_data[OBSERVATION_2] = np.nan

    final_data = merge_by_similar_name(combined_data, report_kustomer, NAME, KUSTOMER_NAME)
    
    return final_data