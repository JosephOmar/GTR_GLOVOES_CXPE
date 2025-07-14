from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware  # <-- Importar middleware CORS
from sqlmodel import select
from typing import Annotated
from sqlmodel import Session
from app.database.database import get_session, create_db_and_tables
from app.routers import worker
from app.routers import upload_planned_data
from app.routers import operational_view
from app.routers import schedule, auth, protected
from app.routers import google_sheets_proxy

# Iniciar FastAPI
app = FastAPI()

# Configuración CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:4321", "https://gtr-cx-glovo-es.netlify.app", "*"],  # Aquí coloca la URL de tu frontend (ejemplo React/Astro)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(worker.router)
app.include_router(upload_planned_data.router)
app.include_router(operational_view.router)
app.include_router(schedule.router)
app.include_router(auth.router)
app.include_router(protected.router)
app.include_router(google_sheets_proxy.router)

# Crear la base de datos y las tablas cuando se inicia la aplicación
create_db_and_tables()  # Llamada inicial

# Dependencia para obtener la sesión de la base de datos
SessionDep = Annotated[Session, Depends(get_session)]


@app.get("/")
async def root():
    return {"message": "Hello World"}
