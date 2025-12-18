from typing import Optional, List
from datetime import date
from sqlmodel import SQLModel

class ContactsReceivedReasonRead(SQLModel):
    contact_reason: str
    count: int

class ContactsReceivedRead(SQLModel):
    team: str

    date_pe: Optional[date]
    interval_pe: str

    date_es: Optional[date]
    interval_es: str

    contacts_received: int

    contact_reasons: List[ContactsReceivedReasonRead] = []



