# app/crud/planned.py
from typing import Dict, Tuple, List
from datetime import date
from sqlmodel import Session, select

from app.models.real_time_data import RealTimeData

Key = Tuple[str, date, str]

def get_planned_map_by_date(session: Session, target_date: date) -> Dict[Key, RealTimeData]:
    """
    Devuelve un dict mapping (team, date, interval) → RealTimeData
    para todos los registros de `target_date`.
    """
    stmt = select(RealTimeData).where(RealTimeData.date == target_date)
    records = session.exec(stmt).all()
    return {
        (r.team, r.date, r.interval): r
        for r in records
    }

def bulk_create_real_time_data(session: Session, new_records: List[RealTimeData]) -> None:
    """
    Inserta en bloque todos los objetos nuevos.
    """
    session.bulk_save_objects(new_records)

def get_all_real_time_data(session: Session) -> List[RealTimeData]:
    """Lee todos los registros."""
    stmt = select(RealTimeData)
    return session.exec(stmt).all()
