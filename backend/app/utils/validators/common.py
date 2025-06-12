from fastapi import HTTPException
from typing import Mapping

def validate_and_map_filename(
    file_name: str,
    name_mapping: Mapping[str, str],
    error_detail_no_keyword: str = "El archivo debe tener un nombre válido.",
    error_detail_bad_ext: str = "El archivo debe ser un archivo Excel (.xlsx) o CSV (.csv)."
) -> str:
    """
    Valida que `file_name` contenga alguna de las claves de name_mapping
    y que termine en .xlsx o .csv. Luego renombra según name_mapping
    y sustituye espacios por '_'.
    """
    # 1) Debe contener al menos una clave
    if not any(keyword in file_name for keyword in name_mapping):
        raise HTTPException(status_code=400, detail=error_detail_no_keyword)

    # 2) Extensión válida
    if not (file_name.endswith('.xlsx') or file_name.endswith('.csv')):
        raise HTTPException(status_code=400, detail=error_detail_bad_ext)

    # 3) Reemplazar usando el mapeo (keywords más largas primero para evitar solapamientos)
    for keyword in sorted(name_mapping, key=len, reverse=True):
        if keyword in file_name:
            file_name = file_name.replace(keyword, name_mapping[keyword])
            break

    # 4) Espacios → guiones bajos
    return file_name.replace(" ", "_")
