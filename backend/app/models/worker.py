from sqlmodel import SQLModel, Field, Relationship
from typing import Optional, List
from datetime import date, time

class Role(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=20)

    workers: List["Worker"] = Relationship(back_populates="role")

class Status(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=20)

    workers: List["Worker"] = Relationship(back_populates="status")

class Campaign(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=20)

    workers: List["Worker"] = Relationship(back_populates="campaign")

class Team(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=20)

    workers: List["Worker"] = Relationship(back_populates="team")

class WorkType(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=20)

    workers: List["Worker"] = Relationship(back_populates="work_type")

class ContractType(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=20)

    workers: List["Worker"] = Relationship(back_populates="contract_type")

class Worker(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    document: str = Field(max_length=10)
    name: str = Field(max_length=60)
    role_id: Optional[int] = Field(default=None, foreign_key="role.id")
    status_id: Optional[int] = Field(default=None, foreign_key="status.id")
    campaign_id: Optional[int] = Field(default=None, foreign_key="campaign.id")
    team_id: Optional[int] = Field(default=None, foreign_key="team.id")
    manager: str = Field(default=None, max_length=60, nullable=True)
    supervisor: str = Field(default=None, max_length=60, nullable=True)
    coordinator: str = Field(default=None, max_length=60, nullable=True)
    work_type_id: Optional[int] = Field(default=None, foreign_key="worktype.id")
    start_date: Optional[date] = Field(default=None)
    termination_date: Optional[date] = Field(default=None)
    contract_type_id: Optional[int] = Field(default=None, foreign_key="contracttype.id")
    requirement_id: Optional[str] = Field(default=None, max_length=15)
    kustomer_id: Optional[str] = Field(default=None, max_length=40)
    kustomer_name: Optional[str] = Field(default=None, max_length=60)
    kustomer_email: Optional[str] = Field(default=None, max_length=60)
    observation_1: Optional[str] = Field(default=None, max_length=60)
    observation_2: Optional[str] = Field(default=None, max_length=60)
    tenure: Optional[int] = Field(default=None)
    trainee: Optional[str] = Field(default=None, max_length=20)

    # Relación uno a muchos con Role
    role: "Role" = Relationship(back_populates="workers")

    # Relación uno a muchos con WorkSetting
    contract_type: "ContractType" = Relationship(back_populates="workers")

    # Relación uno a muchos con WorkType
    work_type: "WorkType" = Relationship(back_populates="workers")

    # Relación uno a muchos con Team
    team: "Team" = Relationship(back_populates="workers")

    # Relación uno a muchos con Campaign
    campaign: "Campaign" = Relationship(back_populates="workers")

    # Relación uno a muchos con Status
    status: "Status" = Relationship(back_populates="workers")

class Absence(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    worker_id: int = Field(foreign_key="worker.id")
    date: date
    login_time: time
    absence_reason: time

class Attendance(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    worker_id: int = Field(foreign_key="worker.id")
    date: date
    login_time: time
    connection_time: time
    break_start: time

class UbycallSchedule(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    worker_id: int = Field(foreign_key="worker.id")
    date: date
    start_time: time
    end_time: time

class Schedule(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    worker_id: int = Field(foreign_key="worker.id")
    date: date
    start_time: time
    end_time: time
    break_end: time
    is_rest_day: bool
