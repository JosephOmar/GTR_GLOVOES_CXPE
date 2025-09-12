from typing import Optional, List
from datetime import date
from sqlmodel import SQLModel
from app.schemas.schedule import ScheduleRead, UbycallScheduleRead

class RoleRead(SQLModel):
    name: str

class StatusRead(SQLModel):
    name: str

class CampaignRead(SQLModel):
    name: str

class TeamRead(SQLModel):
    name: str

class WorkTypeRead(SQLModel):
    name: str

class ContractTypeRead(SQLModel):
    name: str

class WorkerRead(SQLModel):
    document: str
    name: str

    role: Optional[RoleRead]
    status: Optional[StatusRead]
    campaign: Optional[CampaignRead]
    team: Optional[TeamRead]
    work_type: Optional[WorkTypeRead]
    contract_type: Optional[ContractTypeRead]

    manager: Optional[str]
    supervisor: Optional[str]
    coordinator: Optional[str]
    start_date: Optional[date]
    termination_date: Optional[date]
    requirement_id: Optional[str]
    kustomer_id: Optional[str]
    kustomer_name: Optional[str]
    kustomer_email: Optional[str]
    observation_1: Optional[str]
    observation_2: Optional[str]
    tenure: Optional[int]
    trainee: Optional[str]
    training_support: Optional[str]

    # aquí los horarios anidados
    schedules:           List[ScheduleRead]          = []
    ubycall_schedules:   List[UbycallScheduleRead]   = []
