import pandas as pd
from app.core.utils.real_data_view.columns_names import TEAM, DATE, TIME_INTERVAL
from app.core.data_cleaning_real_data_view.clean_planned_data import clean_planned_data
from app.core.data_cleaning_real_data_view.clean_assembled_data import clean_assembled_data
from app.core.data_cleaning_real_data_view.clean_kustomer_data import clean_kustomer_data
from app.core.data_cleaning_real_data_view.clean_real_agents import clean_real_agents
from app.core.data_cleaning_real_data_view.clean_looker_data import clean_looker_data


def merge_data_view(
    data_planned: pd.DataFrame,
    data_assembled_chat: pd.DataFrame,
    data_assembled_call: pd.DataFrame,
    data_kustomer_partial_CS: pd.DataFrame,
    data_kustomer_total_CS: pd.DataFrame,
    data_kustomer_partial_RD: pd.DataFrame,
    data_kustomer_total_RD: pd.DataFrame,
    data_real_agents: pd.DataFrame,
    data_looker_CR: pd.DataFrame,
    data_looker_RD: pd.DataFrame,
) -> pd.DataFrame:

    # --- 1) Limpiar cada fuente ---
    data_planned = clean_planned_data(data_planned)
    data_assembled = clean_assembled_data(
        data_assembled_chat, data_assembled_call)
    data_looker = clean_looker_data(data_looker_CR, data_looker_RD)
    data_kustomer = clean_kustomer_data(
        data_kustomer_partial_CS,
        data_kustomer_total_CS,
        data_kustomer_partial_RD,
        data_kustomer_total_RD
    )
    data_real_agents = clean_real_agents(data_real_agents)

    # --- 2) Merge de ensamblados, kustomer y real_agents por TIME_INTERVAL y TEAM ---
    df_temp = data_assembled.copy()
    df_temp = pd.merge(df_temp, data_kustomer, on=[
                       TIME_INTERVAL, TEAM], how='outer')
    df_temp = pd.merge(df_temp, data_looker, on=[
                       TIME_INTERVAL, TEAM], how='outer')
    print(df_temp[DATE])
    print(data_real_agents[DATE])
    df_temp = pd.merge(df_temp, data_real_agents, on=[
                       DATE, TIME_INTERVAL, TEAM], how='outer')
    # --- 3) Merge con data_planned por DATE (outer para conservar todas las planned dates) ---
    df_merged = pd.merge(
        data_planned,
        df_temp,
        on=[DATE, TIME_INTERVAL, TEAM],
        how='outer'
    )
    # --- 4) Ordenar y resetear índice ---
    df_merged = df_merged.sort_values(by=[DATE, TEAM, TIME_INTERVAL]) \
                         .reset_index(drop=True)   
    return df_merged