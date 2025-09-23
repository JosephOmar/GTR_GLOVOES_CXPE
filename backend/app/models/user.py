
from sqlmodel import SQLModel, Field
from sqlalchemy.orm import relationship

class User(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    name: str
    lastname: str
    email: str
    password: str
