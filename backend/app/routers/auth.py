# app/routers/auth.py

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.database.database import get_session
from app.services.auth_service import authenticate_user
from app.schemas.user import UserRead, UserCreate
from app.models.user import User
from app.utils.security import hash_password
from app.utils.security import verify_password
from app.utils.jwt import create_access_token
import os
from dotenv import load_dotenv

router = APIRouter()
load_dotenv()

class Login(BaseModel):
    username: str
    password: str


@router.post("/login")
async def login(credentials: Login, session: Session = Depends(get_session)):
    # Buscamos al usuario por nombre de usuario
    user = session.query(User).filter(
        User.username == credentials.username).first()

    # Verificamos si la contraseña es correcta
    if user and verify_password(credentials.password, user.hashed_password):
        # Generamos el token JWT
        token = create_access_token({"sub": user.username})
        return {"access_token": token, "token_type": "bearer"}
    else:
        raise HTTPException(status_code=401, detail="Invalid credentials")

# Cargar la lista blanca desde las variables de entorno


def get_whitelisted_emails():
    emails = os.getenv('WHITELISTED_EMAILS', '')
    return emails.split(',')

# Ruta para registrar un nuevo usuario


@router.post("/register", response_model=UserRead)
async def register(user: UserCreate, session: Session = Depends(get_session)):
    # Obtener la lista blanca de correos electrónicos
    whitelisted_emails = get_whitelisted_emails()
    # Verificar si el correo del usuario está en la lista blanca
    if user.username not in whitelisted_emails:
        raise HTTPException(status_code=403, detail="You are not authorized to register")
    
    # Comprobar si el usuario ya existe en la base de datos
    db_user = session.query(User).filter(User.username == user.username).first()
    if db_user:
        raise HTTPException(status_code=400, detail="El nombre de usuario ya está en uso")

    # Encriptar la contraseña del usuario
    hashed_password = hash_password(user.password)

    # Crear el nuevo usuario con el nombre de usuario y la contraseña encriptada
    new_user = User(username=user.username, hashed_password=hashed_password)

    # Guardar el nuevo usuario en la base de datos
    session.add(new_user)
    session.commit()
    session.refresh(new_user)

    # Retornar el nuevo usuario creado
    return new_user
