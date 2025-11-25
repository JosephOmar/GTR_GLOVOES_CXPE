from typing import Optional, List
from datetime import date
from sqlmodel import SQLModel

class SlaBreachedRead(SQLModel, from_attributes=True):
    team: str
    date: Optional[date]
    interval: str
    api_email: Optional[str]
    chat_breached: int
    link: Optional[List[str]] = []