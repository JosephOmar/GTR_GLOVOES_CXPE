from sqlmodel import SQLModel, Field
from sqlalchemy.orm import relationship
from typing import Optional
from datetime import date

class Planned(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    team: str
    date: Optional[date]
    interval: str = Field(max_length=10)
    forecast_received: Optional[int] = Field(default=0)
    required_agents: Optional[int] = Field(default=0)
    scheduled_agents: Optional[int] = Field(default=0)
