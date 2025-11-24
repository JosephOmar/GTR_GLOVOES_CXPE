from typing import Dict, List, Optional
from sqlmodel import Session, select
from sqlalchemy.dialects.postgresql import insert
import math
from collections import Counter

from app.models.worker import Role, Status, Campaign, Team, WorkType, ContractType, Worker

def upsert_lookup_table(session: Session, Model, values: List[str]) -> Dict[str, int]:
    unique_values = {v.strip() for v in values if v and isinstance(v, str)}
    if not unique_values:
        return {}

    # Inserta los valores (si ya existen, ignora)
    stmt = insert(Model).values([{"name": v} for v in unique_values])
    stmt = stmt.on_conflict_do_nothing(index_elements=["name"])
    session.execute(stmt)
    session.commit()

    # Retorna el mapeo actualizado
    all_records = session.exec(select(Model)).all()
    return {r.name: r.id for r in all_records}


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
    Maneja conflictos por document.
    Solo actualiza si hay cambios en los registros existentes.
    """

    if not workers_data:
        return 0

    total_processed = 0

    # Verificar si existe un trabajador con el mismo document en la base de datos
    for worker in workers_data:
        existing_worker = session.query(Worker).filter(Worker.document == worker["document"]).first()

        if existing_worker:
            # Si el trabajador existe, comparar si hay cambios en los datos
            is_updated = False
            for key, value in worker.items():
                if getattr(existing_worker, key) != value:
                    setattr(existing_worker, key, value)  # Actualizar solo si hay un cambio
                    is_updated = True

            if is_updated:
                total_processed += 1
        else:
            # Si no existe, insertamos el nuevo trabajador
            session.add(Worker(**worker))
            total_processed += 1

    # Confirmar cambios
    session.commit()

    print(f"✅ Total registros procesados: {total_processed}")

    return total_processed