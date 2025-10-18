from typing import Optional, List
from datetime import date, time
from sqlmodel import SQLModel


class PlannedRead(SQLModel):
    team: str
    date: Optional[date]
    interval: str
    forecast_tht: int
    forecast_received: int
    required_agents: int
    scheduled_agents: int