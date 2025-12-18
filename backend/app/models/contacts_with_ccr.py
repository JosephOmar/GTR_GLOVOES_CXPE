from sqlmodel import SQLModel, Field, Relationship
from typing import Optional, List
from datetime import date

class ContactsReceived(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    team: str
    date_pe: Optional[date]
    interval_pe: str = Field(max_length=10)
    date_es: Optional[date]
    interval_es: str = Field(max_length=10)
    contacts_received: int = Field(default=0)

    contact_reasons: List["ContactsReceivedReason"] = Relationship(
        back_populates="contacts_received"
)

class ContactsReceivedReason(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    contacts_received_id: int = Field(foreign_key="contactsreceived.id")
    contact_reason: str
    count: int

    contacts_received: Optional[ContactsReceived] = Relationship(
        back_populates="contact_reasons"
)