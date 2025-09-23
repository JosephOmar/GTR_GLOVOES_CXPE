# app/schemas/user.py

from pydantic import BaseModel

class UserCreate(BaseModel):
    name: str
    lastname: str
    email: str
    password: str

class UserRead(BaseModel):
    id: int
    name: str
    lastname: str
    email: str

    class Config:
        orm_mode = True
