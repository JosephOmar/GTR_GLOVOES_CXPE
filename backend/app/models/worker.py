from sqlmodel import SQLModel, Field, Relationship
from typing import Optional, List
from datetime import date, time
import datetime

class Role(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=30, unique=True)

    workers: List["Worker"] = Relationship(back_populates="role")

class Status(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=20, unique=True)

    workers: List["Worker"] = Relationship(back_populates="status")

class Campaign(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=30, unique=True)

    workers: List["Worker"] = Relationship(back_populates="campaign")

class Team(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=30, unique=True)

    workers: List["Worker"] = Relationship(back_populates="team")

class WorkType(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=30, unique=True)

    workers: List["Worker"] = Relationship(back_populates="work_type")

class ContractType(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(max_length=30, unique=True)

    workers: List["Worker"] = Relationship(back_populates="contract_type")

class Worker(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    document: str = Field(unique=True, max_length=10)
    name: str = Field(max_length=100)
    role_id: Optional[int] = Field(default=None, foreign_key="role.id")
    status_id: Optional[int] = Field(default=None, foreign_key="status.id")
    campaign_id: Optional[int] = Field(default=None, foreign_key="campaign.id")
    team_id: Optional[int] = Field(default=None, foreign_key="team.id")
    manager: str = Field(default=None, max_length=100, nullable=True)
    supervisor: str = Field(default=None, max_length=100, nullable=True)
    coordinator: str = Field(default=None, max_length=100, nullable=True)
    work_type_id: Optional[int] = Field(default=None, foreign_key="worktype.id")
    start_date: Optional[date] = Field(default=None)
    termination_date: Optional[date] = Field(default=None)
    contract_type_id: Optional[int] = Field(default=None, foreign_key="contracttype.id")
    requirement_id: Optional[str] = Field(default=None, max_length=15)
    api_id: Optional[str] = Field(default=None, max_length=40)
    api_name: Optional[str] = Field(default=None, max_length=100)
    api_email: Optional[str] = Field(default=None, unique=True, max_length=100)
    observation_1: Optional[str] = Field(default=None, max_length=100)
    observation_2: Optional[str] = Field(default=None, max_length=100)
    tenure: Optional[int] = Field(default=None)
    trainee: Optional[str] = Field(default=None, max_length=20)
    productive: Optional[str] = Field(default='No')

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

    #! Relaciones a horarios
    schedules: List["Schedule"] = Relationship(back_populates="worker", sa_relationship_kwargs={"cascade": "all, delete-orphan"})

    ubycall_schedules: List["UbycallSchedule"] = Relationship(back_populates="worker", sa_relationship_kwargs={"cascade": "all, delete-orphan"})

    attendances: List["Attendance"] = Relationship(back_populates="worker")

class UbycallSchedule(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    worker_document: str = Field(foreign_key="worker.document")
    date: date
    day: str
    start_time: Optional[time] = None
    end_time: Optional[time] = None

    # Relación hacia Worker
    worker: Worker = Relationship(back_populates="ubycall_schedules")

class Schedule(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    worker_document: str = Field(foreign_key="worker.document")
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    start_time: Optional[time] = None
    end_time: Optional[time] = None
    break_start: Optional[time] = None
    break_end: Optional[time] = None
    is_rest_day: bool
    obs: Optional[str] = Field(default=None, max_length=4, nullable=True)

    # Relación hacia Worker
    worker: Worker = Relationship(back_populates="schedules")

class Attendance(SQLModel, table=True):
    """
    Registro real de asistencia del trabajador:
    - Marca de entrada y salida.
    - Si asistió o no al turno planificado.
    - Observaciones de puntualidad, faltas, etc.    
    """
    id: Optional[int] = Field(default=None, primary_key=True)
    api_email: str = Field(foreign_key="worker.api_email")

    date: date
    check_in: Optional[time] = None
    check_out: Optional[time] = None
    status: str = Field(default="absent")  
    out_of_adherence: Optional[int] = Field(default=None)
    offline_minutes: Optional[int] = Field(default=None)
    # Ej: "present", "absent", "late", "early_leave"
    # Relación con Worker
    worker: Worker = Relationship(back_populates="attendances")

