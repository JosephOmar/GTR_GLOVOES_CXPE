from sqlmodel import Session, select
from typing import List, Dict
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from app.models.contacts_with_ccr import (
    ContactsReceived,
    ContactsReceivedReason
)

def upsert_contacts_with_ccr(
    session: Session,
    received_data: list[dict],
    reason_data: list[dict],
):

    # =========================
    # 1. UPSERT contacts_received
    # =========================
    stmt_received = insert(ContactsReceived).values(received_data)

    stmt_received = stmt_received.on_conflict_do_update(
        index_elements=[
            ContactsReceived.date_pe,
            ContactsReceived.interval_pe,
            ContactsReceived.date_es,
            ContactsReceived.interval_es,
            ContactsReceived.team,
        ],
        set_={
            "contacts_received": sa.func.greatest(
                stmt_received.excluded.contacts_received,
                ContactsReceived.contacts_received,
            )
        },
    ).returning(ContactsReceived.id,
                ContactsReceived.date_pe,
                ContactsReceived.interval_pe,
                ContactsReceived.date_es,
                ContactsReceived.interval_es,
                ContactsReceived.team)

    result = session.exec(stmt_received).all()

    # Mapa clave → id
    received_id_map = {
        (
            r.date_pe,
            r.interval_pe,
            r.date_es,
            r.interval_es,
            r.team,
        ): r.id
        for r in result
    }

    # =========================
    # 2. Preparar reason_data
    # =========================
    reasons_rows = []

    for row in reason_data:
        key = (
            row["date_pe"],
            row["interval_pe"],
            row["date_es"],
            row["interval_es"],
            row["team"],
        )

        parent_id = received_id_map.get(key)
        if not parent_id:
            continue

        reasons_rows.append({
            "contacts_received_id": parent_id,
            "contact_reason": row["contact_reason"],
            "count": row["count"],
        })

    if not reasons_rows:
        session.commit()
        return "Datos CCR cargados correctamente"

    # =========================
    # 3. UPSERT contacts_received_reason
    # =========================
    stmt_reason = insert(ContactsReceivedReason).values(reasons_rows)

    stmt_reason = stmt_reason.on_conflict_do_update(
        index_elements=[
            ContactsReceivedReason.contacts_received_id,
            ContactsReceivedReason.contact_reason,
        ],
        set_={
            "count": sa.func.greatest(
                stmt_reason.excluded.count,
                ContactsReceivedReason.count,
            )
        },
    )

    session.exec(stmt_reason)
    session.commit()

    return "Datos CCR cargados correctamente"

def get_all_contacts_with_ccr(session: Session):
    stmt = select(ContactsReceived)
    results = session.exec(stmt).all()
    return results
