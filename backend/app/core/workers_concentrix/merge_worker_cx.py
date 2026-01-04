import pandas as pd
from app.core.utils.workers_cx.utils import fuzzy_match
from app.core.workers_concentrix.clean_people_consultation import clean_people_consultation
from app.core.workers_concentrix.clean_scheduling_ppp import clean_scheduling_ppp
from app.core.utils.workers_cx.utils import update_column_based_on_worker
from app.core.utils.workers_cx.columns_names import NAME, API_EMAIL, API_ID, DOCUMENT, SUPERVISOR, REQUIREMENT_ID, TEAM
import numpy as np

def merge_worker_data(df_people_consultation: pd.DataFrame,
                      df_scheduling_ppp: pd.DataFrame) -> pd.DataFrame:

    merged = pd.merge(
        df_people_consultation,
        df_scheduling_ppp,
        on='document',
        how='left',
        suffixes=('_people', '_scheduling')
    ) 

    merged['observation_1'] = merged['observation_1'].fillna('')
    merged['observation_2'] = merged['observation_2'].fillna('')

    merged['team_scheduling'] = merged['team_scheduling'].replace('', np.nan)
    # merged['supervisor_scheduling'] = merged['supervisor_scheduling'].replace('', np.nan)
    merged['requirement_id_scheduling'] = merged['requirement_id_scheduling'].replace('', np.nan)

    merged[TEAM] = merged['team_scheduling'].fillna(merged['team_people'])
    # merged[SUPERVISOR] = merged['supervisor_scheduling'].fillna(merged['supervisor_people'])
    merged[REQUIREMENT_ID] = merged['requirement_id_scheduling'].fillna(merged['requirement_id_people'])

    merged = merged.drop(columns=['team_people', 'team_scheduling','requirement_id_people', 'requirement_id_scheduling'])

    return merged

def merge_worker_data_master(df_people_consultation: pd.DataFrame,
                      master_glovo_cx: pd.DataFrame) -> pd.DataFrame:

    merged = pd.merge(
        df_people_consultation,
        master_glovo_cx,
        on='document',
        how='left',
        suffixes=('_people', '_master')
    ) 

    merged['supervisor_master'] = merged['supervisor_master'].replace('', np.nan)

    merged[SUPERVISOR] = merged['supervisor_master'].fillna(merged['supervisor_people'])

    merged = merged.drop(columns=['supervisor_people', 'supervisor_master'])

    return merged

def merge_by_similar_name(df1: pd.DataFrame, df2: pd.DataFrame, column1: str, column2: str, threshold=95, fallback_threshold=60):
    name_list_df2 = df2[column2].tolist()

    def get_best_match(name):
        best_match = fuzzy_match(name, name_list_df2, threshold)
        if not best_match:
            best_match = fuzzy_match(name, name_list_df2, fallback_threshold)
        
        return best_match

    df1["best_match"] = df1[column1].apply(get_best_match)

    merged_df = pd.merge(df1, df2, left_on="best_match", right_on=column2, how="left")

    merged_df = merged_df.drop(columns=["best_match"])

    return merged_df

def merge_with_despegando(df_final_worker: pd.DataFrame, df_despegando: pd.DataFrame) -> pd.DataFrame:
    df_despegando = df_despegando.rename(columns={c: str(c).strip().lower() for c in df_despegando.columns})
    df_despegando = df_despegando.rename(columns={
        "document": DOCUMENT,
        "supervisor": "supervisor_despegando",
        "requirement_id": "requirement_id_despegando",
    })

    merged = pd.merge(
        df_final_worker,
        df_despegando,
        on=DOCUMENT,
        how="left"
    )

    if "requirement_id_despegando" in merged.columns:
        merged["requirement_id"] = merged["requirement_id_despegando"].combine_first(merged["requirement_id"])
        merged = merged.drop(columns=["requirement_id_despegando"])

    if "supervisor_despegando" in merged.columns:
        merged["supervisor"] = merged["supervisor_despegando"].combine_first(merged["supervisor"])
        merged = merged.drop(columns=["supervisor_despegando"])

    return merged

COLUMNS_MASTER = {
    'DNI' : DOCUMENT,
    'ASIGNACIÓN INTERNA' : SUPERVISOR
}

def generate_worker_cx_table(
    people_active: pd.DataFrame,
    people_inactive: pd.DataFrame,
    scheduling_ppp: pd.DataFrame,
    api_id: pd.DataFrame,
    master_glovo_cx: pd.DataFrame
) -> pd.DataFrame:

    df_people_consultation = clean_people_consultation(people_active, people_inactive)
    df_scheduling_ppp = clean_scheduling_ppp(scheduling_ppp)
    master_glovo_cx = master_glovo_cx.rename(columns=COLUMNS_MASTER)
    master_glovo_cx = master_glovo_cx[list(COLUMNS_MASTER.values())].copy()

    api_id = api_id.rename(columns={
        'DOCUMENT': DOCUMENT,
        'API EMAIL': API_EMAIL,
        'API ID': API_ID
    })
    api_id = api_id[[DOCUMENT, API_EMAIL, API_ID]]
    print(df_people_consultation[SUPERVISOR])
    print(master_glovo_cx)

    for df in [df_people_consultation, df_scheduling_ppp, api_id, master_glovo_cx]:
        if DOCUMENT in df.columns:
            df[DOCUMENT] = (
                df[DOCUMENT]
                .astype(str)
                .str.replace(r"\.0$", "", regex=True)
                .str.strip()
            )

    df_people_and_ppp = merge_worker_data(df_people_consultation, df_scheduling_ppp)
    df_people_and_ppp = merge_worker_data_master(df_people_and_ppp, master_glovo_cx)

    df_final_worker = pd.merge(
        df_people_and_ppp,
        api_id,
        on=DOCUMENT,
        how="left"
    )

    df_final_worker = update_column_based_on_worker(
        df_final_worker,
        df_people_consultation,
        SUPERVISOR,
        NAME
    )

    return df_final_worker
