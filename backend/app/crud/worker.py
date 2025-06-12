from typing import Dict, List, Optional
from sqlmodel import Session, select

from app.models.worker import Role, Status, Campaign, Team, WorkType, ContractType, Worker

def upsert_lookup_table(session: Session, model, names: List[str]) -> Dict[str,int]:
    """
    Inserta (si no existe) en table model para cada name de names
    y retorna un map name→id.
    """
    result = {}
    unique = set(n for n in names if n is not None)
    for name in unique:
        inst = session.exec(select(model).where(model.name == name)).first()
        if not inst:
            inst = model(name=name)
            session.add(inst)
            session.commit()
            session.refresh(inst)
        result[name] = inst.id
    return result


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
            setattr(existing, key, value)
        worker = existing
    else:
        worker = Worker(**data)
        session.add(worker)

    return worker
