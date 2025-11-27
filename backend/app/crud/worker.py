from typing import Dict, List, Optional
from sqlmodel import Session, select
from sqlalchemy.dialects.postgresql import insert
import math
from collections import Counter
import time

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
    Optimizado para evitar consultas repetidas.
    Solo actualiza si hay cambios reales.
    """

    if not workers_data:
        return 0

    total_processed = 0

    # 1. Obtener TODOS los workers existentes en un dict {document: Worker}
    existing_workers = {
        w.document: w
        for w in session.exec(select(Worker)).all()
    }

    workers_to_insert = []

    # 2. Recorrer cada worker entrante y decidir si actualizar o insertar
    for worker in workers_data:
        doc = worker["document"]

        if doc in existing_workers:
            # --- UPDATE ---
            existing_worker = existing_workers[doc]
            is_updated = False

            for key, value in worker.items():
                if hasattr(existing_worker, key):
                    if getattr(existing_worker, key) != value:
                        setattr(existing_worker, key, value)
                        is_updated = True

            if is_updated:
                total_processed += 1

        else:
            # --- INSERT ---
            workers_to_insert.append(worker)

    # 3. Insertar todos los nuevos workers (si existen)
    if workers_to_insert:
        workers_objects = [Worker(**w) for w in workers_to_insert]
        session.bulk_save_objects(workers_objects)
        total_processed += len(workers_to_insert)

    # 4. Guardar cambios
    session.commit()

    print(f"✅ Total registros procesados: {total_processed}")
    return total_processed

