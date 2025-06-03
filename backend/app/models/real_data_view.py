# planned_and_real_data.py
from sqlmodel import SQLModel, Field, UniqueConstraint
from typing import Optional
from datetime import date

# Modelo para Planned Data
class RealDataView(SQLModel, table=True):
    __table_args__ = (UniqueConstraint("team", "date", "time_interval"),)

    id: Optional[int] = Field(default=None, primary_key=True)
    ###Data Planificada
    team: str = Field(max_length=30)
    time_interval: str = Field(max_length=10)
    date: Optional[date]
    # !Contacs
    forecast_received: Optional[int] = Field(default=0)
    # !Agents
    required_agents: Optional[int] = Field(default=0)
    scheduled_agents: Optional[int] = Field(default=0)
    forecast_hours: Optional[float] = Field(default=0)
    scheduled_hours: Optional[float] = Field(default=0)
    ###Data Assembled
    service_level: Optional[float] = Field(default=0)
    real_received: Optional[int] = Field(default=0)
    ###Data Kustomer
    # !Agents
    agents_online: Optional[int] = Field(default=0)
    agents_training: Optional[int] = Field(default=0)
    agents_aux: Optional[int] = Field(default=0)
    # !SAT
    sat_samples: Optional[int] = Field(default=0)
    sat_ongoing: Optional[float] = Field(default=0)
    sat_promoters: Optional[float] = Field(default=0)
    sat_promoters_monthly: Optional[float] = Field(default=81.00)
    sat_interval: Optional[float] = Field(default=0)
    ###Data Looker
    sat_abuser: Optional[float] = Field(default=0)
    aht: Optional[int] = Field(default=0)







