# planned_and_real_data.py
from sqlmodel import SQLModel, Field
from typing import Optional
from datetime import date

# Modelo para Planned Data
class PlannedData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    team: str = Field(max_length=30)
    week: Optional[int] = Field(default=0)
    time_interval: str = Field(max_length=10)
    date: date
    forecast_received: Optional[int] = Field(default=0)
    forecast_aht: Optional[int] = Field(default=0)
    forecast_absenteeism: Optional[int] = Field(default=0)
    required_agents: Optional[int] = Field(default=0)
    scheduled_concentrix: Optional[int] = Field(default=0)
    scheduled_ubycalls: Optional[int] = Field(default=0)
    forecast_hours: Optional[float] = Field(default=0)
    scheduled_hours: Optional[float] = Field(default=0)

# Modelo para Real Data
class RealData(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    team: str = Field(max_length=30)
    week: int
    time_interval: str = Field(max_length=10)
    date: date
    real_received: int
    real_attended: int
    real_missed: int
    real_abandoned: int
    real_aht: int
    real_sla_avr: float
    real_sla_frt: float
    sat: float
    qsat: int
    sat_5: float
    real_concentrix: int
    real_ubycalls: int
    real_holding_time: int
    real_acw_time: int
