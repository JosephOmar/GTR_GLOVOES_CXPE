from typing import List, Dict
from datetime import date
from fastapi import APIRouter, UploadFile, Depends, File, Form, HTTPException
from sqlmodel import Session, select

from app.database.database import get_session
from app.services.schedule_service import process_and_persist_schedules
from app.models.worker import Schedule, UbycallSchedule
from app.routers.utils.google_drive_utils import get_public_drive_files, download_drive_file

router = APIRouter(tags=["schedules"])

@router.post("/upload-schedules/", summary="Carga y persiste horarios desde Excel")
async def upload_schedules(
    files: List[UploadFile] = File(...),
    week: int = Form(...),                 # 👈 obligatorio, viene del form-data
    year: int | None = Form(None),         # 👈 opcional, también del form-data
    session: Session = Depends(get_session),
):
    """
    - Recibe dos Excel (concentrix y ubycall).
    - Inserta sólo los schedules cuyos DOCUMENT existen en Worker.
    - Devuelve cuántos se insertaron y lista de documentos faltantes.
    """
    result = await process_and_persist_schedules(
        files=files,
        session=session,
        week=week,
        year=year,
    )
    return {
        "message": (
            f"Se insertaron {result['inserted_concentrix']} horarios Concentrix "
            f"y {result['inserted_ubycall']} horarios Ubycall."
        ),
        "missing_worker_documents": result["missing_workers"]
    }

@router.get(
    "/schedules/today",
    summary="Obtiene los horarios de hoy (Concentrix y Ubycall)"
)
def read_today_schedules(session: Session = Depends(get_session)) -> Dict[str, List]:
    """
    Devuelve un JSON con dos listas:
    - 'concentrix_schedules': registros de Schedule con date == hoy.
    - 'ubycall_schedules':    registros de UbycallSchedule con date == hoy.
    """
    today = date.today()

    conc = session.exec(
        select(Schedule).where(Schedule.start_date == today)
    ).all()
    uby = session.exec(
        select(UbycallSchedule).where(UbycallSchedule.date == today)
    ).all()

    return {
        "concentrix_schedules": conc,
        "ubycall_schedules":    uby
    }

# Usando el mismo DRIVE_FOLDER_ID que ya tienes
DRIVE_FOLDER_ID = "1H1B_eqjXupZBPk6VdLJ2aeou4PPyOq-H"

# Config de archivos esperados para schedules
REQUIRED_SCHEDULES = [
    {"label": "Schedule Concentrix", "expectedPart": "schedule_concentrix"},
    {"label": "People Obs", "expectedPart": "people_obs"},
    {"label": "Schedule Ubycall", "expectedPart": "schedule_ubycall"},
    {"label": "Schedule ppp", "expectedPart": "schedule_ppp"},
]


@router.post("/auto-upload-schedules/")
async def auto_upload_schedules(session: Session = Depends(get_session)):
    """
    Descarga automáticamente los horarios desde Google Drive (carpeta pública),
    detecta los 3 archivos esperados por nombre y los procesa.
    La semana/año se calculan dentro de process_and_persist_schedules.
    """
    import time
    start_total = time.perf_counter()
    print("\n⚙️ [AUTO-UPLOAD] Iniciando proceso automático de carga de horarios desde Drive...")

    try:
        # 1️⃣ Consultar archivos públicos
        t1 = time.perf_counter()
        print("🔍 [STEP 1] Consultando archivos públicos en carpeta de Drive...")
        files = get_public_drive_files(DRIVE_FOLDER_ID)
        print(f"📁 [STEP 1] Archivos encontrados: {len(files)} | Tiempo: {time.perf_counter() - t1:.3f}s")

        # 2️⃣ Buscar y descargar cada archivo requerido
        files_to_process: List[UploadFile] = []
        t2 = time.perf_counter()

        for meta in REQUIRED_SCHEDULES:
            found = next(
                (f for f in files if meta["expectedPart"].lower() in f["name"].lower()),
                None,
            )
            if not found:
                raise HTTPException(
                    status_code=404,
                    detail=f"No se encontró el archivo requerido: {meta['label']}",
                )

            print(f"⬇️ [STEP 2] Descargando archivo: {found['name']}")
            download_start = time.perf_counter()
            upload_file = download_drive_file(found["id"], found["name"])
            download_time = time.perf_counter() - download_start
            print(f"   ↳ Descargado en {download_time:.3f}s")
            files_to_process.append(upload_file)

        print(f"✅ [STEP 2] Descarga total completada en {time.perf_counter() - t2:.3f}s")

        # 3️⃣ Procesar archivos (usa la semana actual)
        t3 = time.perf_counter()
        print("🚀 [STEP 3] Ejecutando process_and_persist_schedules()...")
        result = await process_and_persist_schedules(
            files=files_to_process,
            session=session,
            week=None,
            year=None,
        )
        process_time = time.perf_counter() - t3
        print(f"✅ [STEP 3] Proceso de persistencia completado en {process_time:.3f}s")

        # 4️⃣ Fin del proceso
        total_time = time.perf_counter() - start_total
        print(f"🏁 [AUTO-UPLOAD] Proceso total completado en {total_time:.3f}s")

        return {
            "message": (
                f"Se insertaron {result['inserted_concentrix']} horarios Concentrix "
                f"y {result['inserted_ubycall']} horarios Ubycall."
            ),
            "missing_worker_documents": result["missing_workers"],
            "timing": {
                "drive_query_s": round(time.perf_counter() - t1, 3),
                "download_s": round(time.perf_counter() - t2, 3),
                "process_s": round(process_time, 3),
                "total_s": round(total_time, 3),
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        total_time = time.perf_counter() - start_total
        print(f"❌ [EXCEPTION auto-upload-schedules] {str(e)} (tiempo total: {total_time:.3f}s)")
        raise HTTPException(status_code=500, detail=f"Error al procesar los horarios automáticamente. ({str(e)})")