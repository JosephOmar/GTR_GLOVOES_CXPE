from sqlmodel import SQLModel, Field
from sqlalchemy.orm import relationship
from typing import Optional
from datetime import date

class SlaBreached(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    team: str
    date: Optional[date]
    interval: str = Field(max_length=10)
    api_email: Optional[str]
    chat_breached: Optional[int] = Field(default=0)