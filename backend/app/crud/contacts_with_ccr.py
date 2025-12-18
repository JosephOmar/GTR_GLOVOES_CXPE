from sqlmodel import Session, select
from typing import List, Dict
from app.models.contacts_with_ccr import (
    ContactsReceived,
    ContactsReceivedReason
)

def upsert_contacts_with_ccr(
    session: Session,
    received_data: List[Dict],
    reason_data: List[Dict]
):

    # 🔹 Cache para evitar múltiples queries por la misma combinación
    received_cache = {}
    print('xd1')
    print(received_data[10])
    print(reason_data[10])
    # =========================
    # 1. UPSERT CONTACTS RECEIVED
    # =========================
    for row in received_data:
        key = (
            row["date_pe"],
            row["interval_pe"],
            row["date_es"],
            row["interval_es"],
            row["team"],
        )

        stmt = select(ContactsReceived).where(
            ContactsReceived.date_pe == row["date_pe"],
            ContactsReceived.interval_pe == row["interval_pe"],
            ContactsReceived.date_es == row["date_es"],
            ContactsReceived.interval_es == row["interval_es"],
            ContactsReceived.team == row["team"],
        )

        existing = session.exec(stmt).first()

        if existing:
            # 🔹 actualizar solo si el valor es mayor
            if row["contacts_received"] > existing.contacts_received:
                existing.contacts_received = row["contacts_received"]
            received_cache[key] = existing
        else:
            new_received = ContactsReceived(**row)
            session.add(new_received)
            session.flush()  # 👈 necesario para obtener el ID
            received_cache[key] = new_received
    print('xd2')
    # =========================
    # 2. UPSERT CONTACT REASONS
    # =========================
    for row in reason_data:
        key = (
            row["date_pe"],
            row["interval_pe"],
            row["date_es"],
            row["interval_es"],
            row["team"],
        )

        parent = received_cache.get(key)
        if not parent:
            # Seguridad: no debería pasar
            continue

        stmt = select(ContactsReceivedReason).where(
            ContactsReceivedReason.contacts_received_id == parent.id,
            ContactsReceivedReason.contact_reason == row["contact_reason"],
        )

        existing_reason = session.exec(stmt).first()

        if existing_reason:
            if row["count"] > existing_reason.count:
                existing_reason.count = row["count"]
        else:
            new_reason = ContactsReceivedReason(
                contacts_received_id=parent.id,
                contact_reason=row["contact_reason"],
                count=row["count"],
            )
            session.add(new_reason)
    print('xd3')
    session.commit()

    return f"Nuevos datos cargados correctamente"

def get_all_contacts_with_ccr(session: Session):
    stmt = select(ContactsReceived)
    results = session.exec(stmt).all()
    return results
