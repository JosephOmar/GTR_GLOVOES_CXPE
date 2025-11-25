import pandas as pd

def clean_sla_breached(data: pd.DataFrame):
    print("Columnas del DataFrame:", data.columns.tolist())

    data = data[data['status'] == 'CLOSED'].copy()

    data['creation_timestamp_local'] = pd.to_datetime(
        data['creation_timestamp_local'], 
        format='%B %d, %Y, %I:%M %p'
    )
    
    data['date'] = data['creation_timestamp_local'].dt.date
    data['interval'] = data['creation_timestamp_local'].dt.strftime('%H:00')

    if 'Contact Link' not in data.columns:
        raise KeyError("'Contact Link' columna no encontrada en el DataFrame.")

    data = data.rename(columns={'stakeholder': 'team', 'agent_email': 'api_email'})

    # ✅ REPLACE ANTES de ambos groupby
    data['team'] = data['team'].replace({
        'customer': 'Customer Tier1',
        'rider': 'Rider Tier1',
        'vendor': 'Vendor Tier1'
    })

    # ========= 1️⃣ Grupo principal ==========
    data_grouped = data.groupby(
        ['team', 'date', 'interval', 'api_email'],
        as_index=False
    ).size()

    data_grouped = data_grouped.rename(columns={'size': 'chat_breached'})

    # ========= 2️⃣ Grupo para enlaces ==========
    links_grouped = (
        data.groupby(['team', 'date', 'interval', 'api_email'])['Contact Link']
        .apply(lambda x: list(x.unique()))
        .reset_index()
        .rename(columns={'Contact Link': 'link'})
    )

    # ========= 3️⃣ Merge final ==========
    final = data_grouped.merge(
        links_grouped,
        on=['team', 'date', 'interval', 'api_email'],
        how='left'
    )

    return final