from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from sqlmodel import Session
import logging
import traceback
import sys
from typing import Annotated
from app.database.database import get_session
from app.database.migrate import run_migrations
from app.routers import (
    worker,
    operational_view,
    schedule,
    auth,
    protected,
    google_sheets_proxy,
    planned,
    users,
    attendance,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logging.getLogger("uvicorn.error").setLevel(logging.DEBUG)
logging.getLogger("uvicorn.access").setLevel(logging.DEBUG)
logging.getLogger("fastapi").setLevel(logging.DEBUG)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO) 

# --- Nueva forma recomendada: lifespan ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("▶ Ejecutando migraciones de base de datos...")
    run_migrations()
    print("✅ Migraciones completadas.")
    yield  # Aquí inicia la app
    print("🛑 Cerrando aplicación...")  # Aquí puedes liberar recursos si deseas

# Crear la aplicación con lifespan
app = FastAPI(
    title="GTR CX Backend",
    version="1.0.0",
    debug=True,
    lifespan=lifespan,
)

@app.middleware("http")
async def log_exceptions(request: Request, call_next):
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        print("🔥 ERROR DETECTADO EN MIDDLEWARE GLOBAL 🔥")
        print("Ruta:", request.url.path)
        print(traceback.format_exc())
        raise e  # vuelve a lanzar el error para que Uvicorn lo muestre

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:4321",
        "https://gtr-cx-glovo-es.netlify.app",
        "*",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Registrar routers
app.include_router(worker.router)
app.include_router(operational_view.router)
app.include_router(schedule.router)
app.include_router(auth.router)
app.include_router(protected.router)
app.include_router(google_sheets_proxy.router)
app.include_router(planned.router)
app.include_router(users.router)
app.include_router(attendance.router)

# Dependencia para obtener la sesión
SessionDep = Annotated[Session, Depends(get_session)]