from typing import Optional, List
from datetime import date, time
from sqlmodel import SQLModel


class UbycallScheduleRead(SQLModel):
    date: date
    day: str
    start_time: Optional[time]
    end_time:   Optional[time]


class ScheduleRead(SQLModel):
    date:        date
    day:         str
    start_time:  Optional[time]
    end_time:    Optional[time]
    break_start: Optional[time]
    break_end:   Optional[time]
    is_rest_day: bool
    obs:         Optional[str]
