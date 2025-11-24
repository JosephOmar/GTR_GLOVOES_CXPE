from typing import Optional, List
from datetime import date, time
from sqlmodel import SQLModel


class SlaBreachedRead(SQLModel):
    team: str
    date: Optional[date]
    interval: str
    api_email: str
    chat_breached: int
