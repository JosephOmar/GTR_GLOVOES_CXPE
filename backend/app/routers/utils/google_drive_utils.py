from fastapi import UploadFile, HTTPException
import requests
from io import BytesIO
import re
from bs4 import BeautifulSoup

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