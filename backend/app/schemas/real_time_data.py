from typing import Optional, List
from datetime import date, time
from sqlmodel import SQLModel


class RealTimeDataRead(SQLModel):
    team: str
    date: Optional[date]
    interval: str
    contacts_received: int
    sla_frt: float
    tht: float