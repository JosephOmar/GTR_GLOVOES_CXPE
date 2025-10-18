# app/core/migrate.py (por ejemplo)
from alembic import command
from alembic.config import Config
from pathlib import Path

def run_migrations(db_url: str | None = None):
    cfg = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    if db_url:
        cfg.set_main_option("sqlalchemy.url", db_url)
    command.upgrade(cfg, "head")
