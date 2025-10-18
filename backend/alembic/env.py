from logging.config import fileConfig
from sqlalchemy import engine_from_config, pool
from alembic import context
from sqlmodel import SQLModel
import os
import sys
from pathlib import Path

# --- Asegurar que el paquete 'app' se pueda importar ---
ROOT_DIR = Path(__file__).resolve().parents[1]  # carpeta raíz del repo
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

# Importa tus modelos ANTES de target_metadata
from app.models.worker import Worker, Role, Status, Campaign, Team, WorkType, ContractType, Attendance, UbycallSchedule, Schedule
from app.models.planned import Planned
from app.models.user import User

# Config de Alembic
config = context.config

# Permite sobreescribir la URL por variable de entorno o parámetro -x
db_url = os.environ.get("DATABASE_URL") or context.get_x_argument(as_dictionary=True).get("db_url")
if db_url:
    config.set_main_option("sqlalchemy.url", db_url)

# Logging
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Meta de SQLModel
target_metadata = SQLModel.metadata


def run_migrations_offline():
    """Modo offline: genera SQL sin conectarse."""
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,        # detectar cambios de tipos
        render_as_batch=True,     # necesario para SQLite
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Modo online: aplica migraciones con conexión."""
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
            compare_type=True,        # detectar cambios de tipos
            render_as_batch=True,     # necesario para SQLite
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
