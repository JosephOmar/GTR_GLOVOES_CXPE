from fastapi import APIRouter, UploadFile, Depends, File, HTTPException
from sqlmodel import Session, select
from sqlalchemy.orm import selectinload, with_loader_criteria
from typing import List
import requests
from io import BytesIO
import re
from bs4 import BeautifulSoup

from app.database.database import get_session
from app.services.workers_service import process_and_persist_workers
from app.models.worker import Worker
from app.schemas.worker import WorkerRead
from app.models.worker import Schedule, UbycallSchedule
from app.routers.protected import get_current_user
from app.models.user import User
import pytz
from datetime import datetime, timedelta, timezone

router = APIRouter()

@router.post("/upload-workers/")
async def upload_workers(
    files: List[UploadFile] = File(...),
    session: Session = Depends(get_session)
):
    count = await process_and_persist_workers(
        files,
        session
    )
    return {"message": f"Se insertaron {count} trabajadores correctamente."}

@router.get(
    "/workers/",
    response_model=List[WorkerRead],
    summary="Lista todos los trabajadores con sus horarios"
)
def read_workers(session: Session = Depends(get_session), current_user: User = Depends(get_current_user)):
    # Cargamos role, status, campaign, etc. y además schedules y ubycall_schedules
    peru_tz = timezone(timedelta(hours=-5))
    current_day = datetime.now(peru_tz).date()
    previous_day = current_day - timedelta(days=1)
    statement = (
        select(Worker)
        .options(
            selectinload(Worker.role),
            selectinload(Worker.status),
            selectinload(Worker.campaign),
            selectinload(Worker.team),
            selectinload(Worker.work_type),
            selectinload(Worker.contract_type),
            selectinload(Worker.schedules),
            selectinload(Worker.ubycall_schedules),
            selectinload(Worker.attendances),
            with_loader_criteria(
                Schedule,
                Schedule.date.in_([current_day, previous_day])
            ),
            with_loader_criteria(
                UbycallSchedule,
                UbycallSchedule.date.in_([current_day, previous_day])
            ),
        )
    )
    workers = session.exec(statement).all()

    # for w in workers:
    #     w.schedules = [s for s in w.schedules if s.date == current_day]
    #     w.ubycall_schedules = [u for u in w.ubycall_schedules if u.date == current_day]

    return workers

# ==============================================================
# CONFIGURACIÓN
# ==============================================================

# ID de carpeta pública (de tu enlace)
DRIVE_FOLDER_ID = "1PTEpHEVbY_PpeT_9Cb0Rb3v1IC3MZBRZ"

# Archivos esperados (por parte del nombre)
REQUIRED_FILES = [
    {"label": "People Active", "expectedPart": "people_active"},
    {"label": "People Inactive", "expectedPart": "people_inactive"},
    {"label": "Scheduling PPP", "expectedPart": "scheduling_ppp"},
    {"label": "API ID", "expectedPart": "api_id"},
    {"label": "Master Glovo", "expectedPart": "master_glovo"},
    {"label": "Scheduling Ubycall", "expectedPart": "scheduling_ubycall"},
]


# ==============================================================
# FUNCIONES AUXILIARES
# ==============================================================

def get_public_drive_files(folder_id: str):
    """
    Lee la vista embebida de la carpeta pública de Drive y extrae (id, name)
    de cada archivo listado.
    """
    url = f"https://drive.google.com/embeddedfolderview?id={folder_id}#list"
    res = requests.get(url)

    if res.status_code != 200:
        raise HTTPException(status_code=500, detail="No se pudo acceder a la carpeta de Google Drive.")

    soup = BeautifulSoup(res.text, "html.parser")

    # Selecciona todos los <a> dentro de flip-entries que apunten a /file/d/<ID>/
    links = soup.select("div.flip-entries a[href*='/file/d/']")

    files = []
    for a in links:
        href = a.get("href", "")
        m = re.search(r"/file/d/([^/]+)/", href)
        if not m:
            continue

        file_id = m.group(1)

        title_el = a.select_one(".flip-entry-title")
        name = title_el.get_text(strip=True) if title_el else file_id

        files.append({"id": file_id, "name": name})

    if not files:
        # Debug en caso vuelva a fallar
        with open("drive_debug.html", "w", encoding="utf-8") as f:
            f.write(res.text)
        print("⚠️ No se encontraron archivos. HTML guardado en drive_debug.html")
        raise HTTPException(status_code=500, detail="No se pudieron obtener los archivos de Google Drive.")

    print(f"📦 Se detectaron {len(files)} archivos en la carpeta pública de Drive:")
    for f in files:
        print(f"   - {f['name']} ({f['id']})")

    return files

def download_drive_file(file_id: str, filename: str) -> UploadFile:
    """Descarga un archivo público de Google Drive y lo devuelve como UploadFile."""
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    res = requests.get(url)

    if res.status_code != 200:
        raise HTTPException(status_code=500, detail=f"No se pudo descargar el archivo {filename}")

    # Importante: no pasar content_type si tu UploadFile no lo soporta
    return UploadFile(
        filename=filename,
        file=BytesIO(res.content),
    )


# ==============================================================
# ENDPOINT PRINCIPAL
# ==============================================================

@router.post("/auto-upload-workers/")
async def auto_upload_workers(session: Session = Depends(get_session)):
    """
    Descarga automáticamente los archivos desde Google Drive
    buscándolos por nombre dentro de una carpeta pública.
    """
    try:
        print("🔍 [STEP 1] Consultando archivos públicos en carpeta de Drive...")
        files = get_public_drive_files(DRIVE_FOLDER_ID)

        files_to_process: List[UploadFile] = []

        for meta in REQUIRED_FILES:
            # Buscar el archivo cuyo nombre contenga la palabra esperada
            found = next(
                (f for f in files if meta["expectedPart"].lower() in f["name"].lower()), None
            )
            if not found:
                raise HTTPException(
                    status_code=404,
                    detail=f"No se encontró el archivo requerido: {meta['label']}",
                )

            print(f"⬇️  Descargando archivo: {found['name']}")
            upload_file = download_drive_file(found["id"], found["name"])
            files_to_process.append(upload_file)

        print("🚀 [STEP 2] Procesando archivos con process_and_persist_workers()...")
        count = await process_and_persist_workers(files_to_process, session)

        print(f"✅ Se insertaron {count} trabajadores correctamente.")
        return {"message": f"✅ Se insertaron {count} trabajadores correctamente."}

    except Exception as e:
        print("❌ [EXCEPTION]", str(e))
        raise HTTPException(status_code=500, detail=str(e))
