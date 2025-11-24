from typing import Dict, Tuple, List
from datetime import date
from sqlmodel import Session, select

from app.models.sla_breached import SlaBreached

Key = Tuple[str, date, str, str]  # (team, date, interval, api_email)

def get_sla_breached_map_by_date(session: Session, target_date: date) -> Dict[Key, SlaBreached]:
    """
    Devuelve un dict mapping (team, date, interval, api_email) → SlaBreachedData
    para todos los registros de `target_date`.
    """
    stmt = select(SlaBreached).where(SlaBreached.date == target_date)
    records = session.exec(stmt).all()
    return {
        (r.team, r.date, r.interval, r.api_email): r
        for r in records
    }

def bulk_create_sla_breached_data(session: Session, new_records: List[SlaBreached]) -> None:
    """
    Inserta en bloque todos los objetos nuevos.
    """
    session.bulk_save_objects(new_records)

def get_all_sla_breached_data(session: Session) -> List[SlaBreached]:
    """Lee todos los registros de sla_breached_data."""
    stmt = select(SlaBreached)
    return session.exec(stmt).all()
