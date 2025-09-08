# app/crud/planned.py
from typing import Dict, Tuple, List
from datetime import date
from sqlmodel import Session, select

from app.models.planned import Planned

Key = Tuple[str, date, str]

def get_planned_map_by_date(session: Session, target_date: date) -> Dict[Key, Planned]:
    """
    Devuelve un dict mapping (team, date, interval) → Planned
    para todos los registros de `target_date`.
    """
    stmt = select(Planned).where(Planned.date == target_date)
    records = session.exec(stmt).all()
    return {
        (r.team, r.date, r.interval): r
        for r in records
    }

def bulk_create_planned(session: Session, new_records: List[Planned]) -> None:
    """
    Inserta en bloque todos los objetos Planned nuevos.
    """
    session.bulk_save_objects(new_records)

def get_all_planned(session: Session) -> List[Planned]:
    """Lee todos los registros de Planned."""
    stmt = select(Planned)
    return session.exec(stmt).all()
