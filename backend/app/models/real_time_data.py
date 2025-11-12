from sqlmodel import SQLModel, Field
from sqlalchemy.orm import relationship
from typing import Optional
from datetime import date

class RealTimeData(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    team: str
    date: Optional[date]
    interval: str = Field(max_length=10)
    contacts_received: Optional[int] = Field(default=0)
    sla_frt: Optional[float] = Field(default=0)
    tht: Optional[float] = Field(default=0)
