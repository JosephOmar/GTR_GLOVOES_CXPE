from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware  # <-- Importar middleware CORS
from sqlmodel import select
from typing import Annotated
from sqlmodel import Session
from app.models.worker import Worker
from app.database.database import get_session, create_db_and_tables
from app.routers import upload_workers_cx
from app.routers import upload_planned_data
from app.routers import upload_real_data_view

# Iniciar FastAPI
app = FastAPI()

# Configuración CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4321"],  # Aquí coloca la URL de tu frontend (ejemplo React/Astro)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload_workers_cx.router)
app.include_router(upload_planned_data.router)
app.include_router(upload_real_data_view.router)

# Crear la base de datos y las tablas cuando se inicia la aplicación
create_db_and_tables()  # Llamada inicial

# Dependencia para obtener la sesión de la base de datos
SessionDep = Annotated[Session, Depends(get_session)]


@app.get("/")
async def root():
    return {"message": "Hello World"}
