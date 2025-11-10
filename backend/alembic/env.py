# -*- coding: utf-8 -*-
from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
from sqlmodel import SQLModel
import os
import sys
from pathlib import Path
import sqlmodel 
from dotenv import load_dotenv

# ==========================================================
# CONFIGURACIÓN DE IMPORTS
# ==========================================================
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

env_name = os.getenv("ENVIRONMENT", "development")
env_file = ".env.prod" if env_name == "production" else ".env.dev"
load_dotenv(ROOT_DIR / env_file)

# ==========================================================
# IMPORTACIÓN DE MODELOS
# ==========================================================
from app.models.worker import Worker, Role, Status, Campaign, Team, WorkType, ContractType, Attendance, UbycallSchedule, Schedule
from app.models.planned import Planned
from app.models.user import User
from app.models.data_kpi import PlannedData, RealData

# ==========================================================
# CONFIGURACIÓN BASE DE ALEMBIC
# ==========================================================
config = context.config

# Forzar lectura UTF-8 para fileConfig (Windows-safe)
if config.config_file_name is not None:
    with open(config.config_file_name, encoding="utf-8") as f:
        fileConfig(f)

# ==========================================================
# CONFIGURACIÓN DE URL DE CONEXIÓN DESDE .ENV
# ==========================================================
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Sobrescribe la URL de Alembic con la de entorno
config.set_main_option("sqlalchemy.url", DATABASE_URL)

# Metadatos de SQLModel
target_metadata = SQLModel.metadata
# ==========================================================
# MIGRACIONES OFFLINE / ONLINE
# ==========================================================
def run_migrations_offline():
    """Genera SQL sin conectarse a la BD."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Aplica migraciones conectándose a PostgreSQL."""
    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


# ==========================================================
# EJECUCIÓN
# ==========================================================
if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
