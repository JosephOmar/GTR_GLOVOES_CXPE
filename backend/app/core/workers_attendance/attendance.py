import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from core.utils.attendance.columns_names import RECENT_LOGIN, KUSTOMER_ID, MESSAGES_SENT, TOTAL_TIME

ATTENDANCE_COLUMNS = {
    'Most Recent Login' : RECENT_LOGIN,
    'User ID' : KUSTOMER_ID,
    'Messages Sent' : MESSAGES_SENT,
    'Total Time Logged In': TOTAL_TIME
}

def clean_report(data: pd.DataFrame) -> pd.DataFrame    :
    """
    Lee un CSV y devuelve un DataFrame con los usuarios cuyo
    'Most Recent Login' sea igual o posterior a las 00:00 del día
    actual en hora Perú (UTC−5), incluyendo las columnas:
      - User ID
      - Most Recent Login (datetime UTC)
      - Messages Sent
      - Total Time Logged In  (formato 'X h Y m')
    """
    # 1) Definir zona horaria de Lima
    lima_tz = ZoneInfo('America/Lima')

    data = data.rename(columns={ATTENDANCE_COLUMNS})
    
    # 3) Parsear la columna de login como datetime en UTC
    df[RECENT_LOGIN] = pd.to_datetime(df[RECENT_LOGIN], utc=True)
    
    # 4) Calcular 00:00 de hoy en hora Perú y convertirlo a UTC
    now_lima = datetime.now(lima_tz)
    start_lima = now_lima.replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc = start_lima.astimezone(timezone.utc)
    
    # 5) Filtrar filas con login ≥ 00:00 hoy (hora Perú)
    df = df[df[RECENT_LOGIN] >= start_utc].copy()
    
    # 6) Función auxiliar para convertir ms a "X h Y m"
    def ms_to_h_m(ms):
        secs = ms / 1000
        h = int(secs // 3600)
        m = int((secs % 3600) // 60)
        return f"{h} h {m} m"
    
    # 7) Aplicar conversión sobre la columna de milisegundos
    df[TOTAL_TIME] = df[TOTAL_TIME].apply(ms_to_h_m)
    
    # 8) Devolver solo las columnas requeridas
    return df[[KUSTOMER_ID, RECENT_LOGIN, TOTAL_TIME]]
