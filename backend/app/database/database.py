from sqlmodel import SQLModel, create_engine, Session
from app.models.worker import Worker, Role, Status, Campaign, Team, WorkType, ContractType, Attendance, UbycallSchedule, Schedule
from app.models.data_kpi import PlannedData, RealData
from typing import Generator

# Definición de la URL para la base de datos SQLite
sqlite_file_name = "database.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"

# Configuración del motor de base de datos con los argumentos de conexión
connect_args = {"check_same_thread": False}
engine = create_engine(sqlite_url, connect_args=connect_args)

# Función para crear las tablas
# def create_db_and_tables():
#     SQLModel.metadata.create_all(engine)

# Función para obtener la sesión de base de datos
def get_session() -> Generator[Session, None, None]:
    with Session(engine) as session:
        yield session
