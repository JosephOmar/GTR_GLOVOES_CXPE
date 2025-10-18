from typing import Dict, List, Optional
from sqlmodel import Session, select
import logging

from app.models.worker import Role, Status, Campaign, Team, WorkType, ContractType, Worker

def upsert_lookup_table(session: Session, Model, values: List[str]) -> Dict[str, int]:
    """
    Inserta o actualiza registros de una tabla lookup (como Role, Status, etc.)
    Devuelve un diccionario: { nombre: id }.
    Optimizada para operaciones en lote.
    """
    # Limpiar valores: quitar duplicados y nulos
    unique_values = {v.strip() for v in values if v and isinstance(v, str)}
    if not unique_values:
        return {}

    # Buscar los ya existentes
    existing_records = session.exec(
        select(Model).where(Model.name.in_(unique_values))
    ).all()
    existing_map = {rec.name: rec.id for rec in existing_records}

    # Identificar los nuevos
    missing_values = unique_values - set(existing_map.keys())
    new_records = [Model(name=value) for value in missing_values]

    if new_records:
        session.add_all(new_records)
        session.flush()  # Asigna IDs sin hacer commit aún

        # Agregar los nuevos al mapa
        for record in new_records:
            existing_map[record.name] = record.id

    return existing_map


def upsert_worker(session: Session, data: dict) -> Worker:
    """
    Inserta o actualiza un Worker según su `document`.
    Devuelve la instancia gestionada por SQLModel.
    """
    stmt = select(Worker).where(Worker.document == data["document"])
    existing: Optional[Worker] = session.exec(stmt).first()

    if existing:
        # Actualizar sólo los campos que vengan en data
        for key, value in data.items():
            if hasattr(existing, key):
                setattr(existing, key, value)
        worker = existing
    else:
        worker = Worker(**data)
        session.add(worker)

    return worker

def bulk_upsert_workers(session: Session, workers_data: List[Dict]) -> int:
    """
    Inserta o actualiza múltiples Workers.
    Devuelve la cantidad total de registros procesados.
    """
    documents = [w["document"] for w in workers_data]
    
    # Trae todos los Workers existentes en una sola consulta
    existing_workers = session.exec(select(Worker).where(Worker.document.in_(documents))).all()
    existing_map = {w.document: w for w in existing_workers}

    count_new = 0
    count_updated = 0

    for data in workers_data:
        doc = data["document"]
        existing = existing_map.get(doc)

        if existing:
            updated = False
            for key, value in data.items():
                if hasattr(existing, key):
                    current_value = getattr(existing, key)
                    if current_value != value:
                        setattr(existing, key, value)
                        updated = True

            if updated:
                session.add(existing)
                count_updated += 1
        else:
            worker = Worker(**data)
            session.add(worker)
            count_new += 1

    session.commit()

    logging.info(f"{count_new} nuevos, {count_updated} actualizados.")
    return count_new + count_updated
