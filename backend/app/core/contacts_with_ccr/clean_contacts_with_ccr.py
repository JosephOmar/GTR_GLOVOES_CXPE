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

COUNTRY = ['ES', 'PT']

def clean_contacts_with_ccr(data: pd.DataFrame):

    # ─────────────────────
    # Limpieza básica
    # ─────────────────────
    data = data[data['queue_name'].isin(QUEUE_NAMES.keys())].copy()
    data = data[data['country'].isin(COUNTRY)].copy()
    data['resolution_time'] = pd.to_numeric(data['resolution_time'], errors='coerce')
    data = data[data['resolution_time'] != 0].copy()
    # ─────────────────────
    # Timestamp (UTC)
    # ─────────────────────
    data['creation_timestamp_utc'] = pd.to_datetime(
        data['creation_timestamp_local'],
        format='%B %d, %Y, %I:%M %p',
        utc=True
    )

    mask_dec31_11pm = (
        (data['creation_timestamp_utc'].dt.year == 2025) &
        (data['creation_timestamp_utc'].dt.month == 12) &
        (data['creation_timestamp_utc'].dt.day == 31) &
        (data['creation_timestamp_utc'].dt.hour == 23) &
        (data['queue_name'] == 'VS-case-inbox-spa-ES-tier2')
    )

    dec31_11pm_data = data[mask_dec31_11pm]

    print("📌 Registros 31/12/2025 - 11 PM (UTC):")
    print(dec31_11pm_data[['creation_timestamp_local', 'creation_timestamp_utc', 'ticket_id']])

    print("Cantidad total:", len(dec31_11pm_data))

    # ─────────────────────
    # Zonas horarias
    # ─────────────────────
    data['timestamp_pe'] = data['creation_timestamp_utc'].dt.tz_convert('America/Lima')
    data['timestamp_es'] = data['creation_timestamp_utc'].dt.tz_convert('Europe/Madrid')

    pe_count = dec31_11pm_data.assign(
        timestamp_pe=dec31_11pm_data['creation_timestamp_utc'].dt.tz_convert('America/Lima')
    )

    es_count = dec31_11pm_data.assign(
        timestamp_es=dec31_11pm_data['creation_timestamp_utc'].dt.tz_convert('Europe/Madrid')
    )

    print("\n🇵🇪 Transformados a timestamp_pe:")
    print(pe_count[['creation_timestamp_utc', 'timestamp_pe']])
    print("Cantidad PE:", len(pe_count))

    print("\n🇪🇸 Transformados a timestamp_es:")
    print(es_count[['creation_timestamp_utc', 'timestamp_es']])
    print("Cantidad ES:", len(es_count))

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