from sqlalchemy import Column, ARRAY, String
from sqlmodel import SQLModel, Field
from typing import List, Optional
from datetime import date

class SlaBreached(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    team: str
    date: Optional[date]
    interval: str = Field(max_length=10)
    api_email: Optional[str]
    chat_breached: Optional[int] = Field(default=0)

    # ARRAY de STRING correcto
    link: Optional[List[str]] = Field(
        default=None, 
        sa_column=Column(ARRAY(String), nullable=True)
    )
