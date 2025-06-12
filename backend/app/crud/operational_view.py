# app/crud/real_data_view.py
from typing import Dict, Tuple, List
from datetime import date
from sqlmodel import Session, select

from app.models.operational_view import OperationalView

Key = Tuple[str, date, str]

def get_views_map_by_date(session: Session, target_date: date) -> Dict[Key, OperationalView]:
    """
    Devuelve un dict mapping (team, date, time_interval) → OperationalView
    para todos los registros de `target_date`.
    """
    stmt = select(OperationalView).where(OperationalView.date == target_date)
    records = session.exec(stmt).all()
    return {
        (r.team, r.date, r.time_interval): r
        for r in records
    }

def bulk_create_views(session: Session, new_views: List[OperationalView]) -> None:
    """
    Inserta en bloque todos los objetos OperationalView nuevos.
    """
    session.bulk_save_objects(new_views)

def get_all_views(session: Session) -> List[OperationalView]:
    """Lee todos los registros de OperationalView."""
    stmt = select(OperationalView)
    return session.exec(stmt).all()
