from sqlmodel import SQLModel, create_engine, Session
from typing import Generator
from dotenv import load_dotenv
import os

# ==========================================================
# CONFIGURACIÓN DE CONEXIÓN A POSTGRESQL
# ==========================================================
env_name = os.getenv("ENVIRONMENT", "development")
env_file = ".env.prod" if env_name == "production" else ".env.dev"
load_dotenv(env_file, override=True)

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Crea el motor de conexión
engine = create_engine(DATABASE_URL, echo=False)

print(POSTGRES_USER)
# ==========================================================
# SESIÓN DE BASE DE DATOS
# ==========================================================
def get_session() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session
