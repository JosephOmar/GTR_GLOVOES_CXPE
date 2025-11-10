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
    Maneja conflictos tanto por api_email (cuando existe) como por document (cuando no hay email).
    """

    if not workers_data:
        return 0

    # === 0️⃣ Limpieza de NaN ===
    for w in workers_data:
        for k, v in w.items():
            if isinstance(v, float) and math.isnan(v):
                w[k] = None

    # === 1️⃣ Detección de duplicados ===
    pairs = [(w.get("document"), w.get("api_email")) for w in workers_data]
    duplicates = [pair for pair, count in Counter(pairs).items() if count > 1]
    if duplicates:
        print("⚠️ Duplicados encontrados en workers_data:")
        for d in duplicates:
            print(f"   → document={d[0]} | api_email={d[1]}")

        temp_map = {}
        for w in workers_data:
            key = (w.get("document"), w.get("api_email"))
            temp_map[key] = w
        workers_data = list(temp_map.values())
        print(f"✅ Duplicados eliminados. Total final: {len(workers_data)} registros.")

    # === 2️⃣ Separar por tipo de clave ===
    workers_with_email = [w for w in workers_data if w.get("api_email")]
    workers_without_email = [w for w in workers_data if not w.get("api_email")]

    total_processed = 0

    # === 3️⃣ UPSERT por api_email ===
    if workers_with_email:
        stmt = insert(Worker).values(workers_with_email)
        update_columns = {
            c.name: c
            for c in stmt.excluded
            if c.name not in ("id", "document")
        }
        stmt = stmt.on_conflict_do_update(
            index_elements=["api_email"],
            set_=update_columns,
        )
        session.execute(stmt)
        total_processed += len(workers_with_email)

    # === 4️⃣ UPSERT por document ===
    if workers_without_email:
        stmt2 = insert(Worker).values(workers_without_email)
        update_columns2 = {
            c.name: c
            for c in stmt2.excluded
            if c.name not in ("id", "api_email")
        }
        stmt2 = stmt2.on_conflict_do_update(
            index_elements=["document"],
            set_=update_columns2,
        )
        session.execute(stmt2)
        total_processed += len(workers_without_email)

    # === 5️⃣ Confirmar cambios ===
    session.commit()

    print(f"✅ Total registros procesados: {total_processed}")

    return total_processed