import pandas as pd

QUEUE_NAMES = {
    'VS-case-inbox-spa-ES-tier2': 'Vendor Tier2',
    'CS-case-inbox-spa-ES-tier2': 'Customer Tier2',
    'RS-case-inbox-spa-ES-tier2': 'Rider Tier2',
    'RS-chat-spa-ES-tier1': 'Rider Tier1',
    'VS-chat-spa-ES-tier1': 'Vendor Tier1',
    'CS-chat-spa-ES-live-order': 'Customer Live',
    'CS-chat-spa-ES-nonlive-order': 'Customer Non Live'
}

def clean_contacts_with_ccr(data: pd.DataFrame):

    # ─────────────────────
    # Limpieza básica
    # ─────────────────────
    data = data[data['queue_name'].isin(QUEUE_NAMES.keys())].copy()
    print(data[data['queue_name'] == 'VS-case-inbox-spa-ES-tier2'])
    data['resolution_time'] = pd.to_numeric(data['resolution_time'], errors='coerce')
    data = data[data['resolution_time'] != 0].copy()
    print(data[data['queue_name'] == 'VS-case-inbox-spa-ES-tier2'])
    # ─────────────────────
    # Timestamp (UTC)
    # ─────────────────────
    data['creation_timestamp_utc'] = pd.to_datetime(
        data['creation_timestamp_local'],
        format='%B %d, %Y, %I:%M %p',
        utc=True
    )

    # ─────────────────────
    # Zonas horarias
    # ─────────────────────
    data['timestamp_pe'] = data['creation_timestamp_utc'].dt.tz_convert('America/Lima')
    data['timestamp_es'] = data['creation_timestamp_utc'].dt.tz_convert('Europe/Madrid')

    data['date_pe'] = data['timestamp_pe'].dt.date
    data['interval_pe'] = data['timestamp_pe'].dt.strftime('%H:00')

    data['date_es'] = data['timestamp_es'].dt.date
    data['interval_es'] = data['timestamp_es'].dt.strftime('%H:00')

    # ─────────────────────
    # Team
    # ─────────────────────
    data['team'] = data['queue_name'].map(QUEUE_NAMES)

    # ─────────────────────
    # Contact reason
    # ─────────────────────
    data['contact_reason'] = data['cr_l1']
    data.loc[data['cr_l2'].notna(), 'contact_reason'] += '/' + data['cr_l2']
    data.loc[data['cr_l3'].notna(), 'contact_reason'] += '/' + data['cr_l3']

    # ─────────────────────
    # Tabla 1: métricas por intervalo
    # ─────────────────────
    interval_metrics = (
        data.groupby(
            ['date_pe', 'interval_pe', 'date_es', 'interval_es', 'team'],
            as_index=False
        )
        .size()
        .rename(columns={'size': 'contacts_received'})
    )

    # ─────────────────────
    # Tabla 2: conteo por CR
    # ─────────────────────
    cr_table = (
        data.groupby(
            ['date_pe', 'interval_pe', 'date_es', 'interval_es', 'team', 'contact_reason'],
            as_index=False
        )
        .size()
        .rename(columns={'size': 'count'})
    )

    return interval_metrics, cr_table