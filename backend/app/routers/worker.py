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
from app.routers.utils.google_drive_utils import get_public_drive_files, download_drive_file

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

    try:
        print('xd')
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
                    Schedule.start_date.in_([current_day, previous_day])
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
    except Exception as e:
        print('xd')
        print(e)
        print('xd')

# ==============================================================
# CONFIGURACIÓN
# ==============================================================

# ID de carpeta pública (de tu enlace)
DRIVE_FOLDER_ID = "1PTEpHEVbY_PpeT_9Cb0Rb3v1IC3MZBRZ"

# Archivos esperados (por parte del nombre)
REQUIRED_FILES = [
    {"label": "People Active",        "expectedPart": "people_active"},
    {"label": "People Inactive",      "expectedPart": "people_inactive"},
    {"label": "Scheduling PPP",       "expectedPart": "scheduling_ppp"},
    {"label": "API ID",               "expectedPart": "api_id"},
    {"label": "Master Glovo",         "expectedPart": "master_glovo"},
    {"label": "Scheduling Ubycall",   "expectedPart": "scheduling_ubycall"},
]

@router.post("/auto-upload-workers/")
async def auto_upload_workers(session: Session = Depends(get_session)):
    """
    Descarga automáticamente los archivos desde Google Drive
    buscándolos por nombre dentro de una carpeta pública y
    procesa/actualiza los trabajadores.
    """
    import time
    start_total = time.perf_counter()
    print("\n⚙️ [WORKERS AUTO] Iniciando carga automática de workers desde Drive...")

    try:
        # 1️⃣ Consultar archivos públicos
        t1 = time.perf_counter()
        print("🔍 [W-STEP 1] Consultando archivos públicos en carpeta de Drive...")
        files = get_public_drive_files(DRIVE_FOLDER_ID)
        print(f"📁 [W-STEP 1] Archivos encontrados: {len(files)} | Tiempo: {time.perf_counter() - t1:.3f}s")

        # 2️⃣ Buscar y descargar archivos requeridos
        files_to_process: List[UploadFile] = []
        t2 = time.perf_counter()

        for meta in REQUIRED_FILES:
            found = next(
                (f for f in files if meta["expectedPart"].lower() in f["name"].lower()),
                None,
            )
            if not found:
                msg = f"No se encontró el archivo requerido: {meta['label']}"
                print(f"❌ [W-STEP 2] {msg}")
                raise HTTPException(status_code=404, detail=msg)

            print(f"⬇️ [W-STEP 2] Descargando archivo: {found['name']}")
            dl_start = time.perf_counter()
            upload_file = download_drive_file(found["id"], found["name"])
            dl_time = time.perf_counter() - dl_start
            print(f"   ↳ Descargado en {dl_time:.3f}s")
            files_to_process.append(upload_file)

        print(f"✅ [W-STEP 2] Descarga total completada en {time.perf_counter() - t2:.3f}s")

        # 3️⃣ Procesar e insertar/actualizar workers
        t3 = time.perf_counter()
        print("🚀 [W-STEP 3] Ejecutando process_and_persist_workers()...")
        total_processed = await process_and_persist_workers(files_to_process, session)
        process_time = time.perf_counter() - t3
        print(f"✅ [W-STEP 3] Proceso de persistencia completado en {process_time:.3f}s")

        # 4️⃣ Totales
        total_time = time.perf_counter() - start_total
        print(f"🏁 [WORKERS AUTO] Proceso total completado en {total_time:.3f}s")

        return {
            "message": f"✅ Se insertaron/actualizaron {total_processed} trabajadores correctamente.",
            "timing": {
                "drive_list_s": round(time.perf_counter() - t1, 3),
                "download_s": round(time.perf_counter() - t2, 3),
                "process_s": round(process_time, 3),
                "total_s": round(total_time, 3),
            },
        }

    except HTTPException:
        # Ya logueado arriba
        raise
    except Exception as e:
        total_time = time.perf_counter() - start_total
        print(f"❌ [WORKERS AUTO][EXCEPTION] {str(e)} | Tiempo total: {total_time:.3f}s")
        raise HTTPException(status_code=500, detail=f"Error al procesar los workers automáticamente. ({str(e)})")

