import io
import pandas as pd
import chardet
from fastapi import UploadFile, HTTPException
from typing import Mapping, List, Tuple, Callable, Any, Dict

# 1) Config centralizada de lectura según keyword
EXCEL_READ_CONFIGS: Dict[str, Dict[str, Any]] = {
    "people_consultation": {},  # default: lee hoja por defecto
    "scheduling_ppp":  {
        "sheet_name": "RESUMEN",
        "header": 2,
        "skiprows": 3,
        "engine": "openpyxl",
    },
    "report_kustomer": {},      # si necesita algo especial, lo pones aquí
    "master_glovo": {
        "sheet_name": "AGENTES_UBY",
        "engine": "openpyxl",
    },
    "scheduling_ubycall": {},   # idem
    "planned_data":    {"sheet_name": "DDPP", "engine": "openpyxl"},
}
    
def read_file_safely(file_stream: io.BytesIO, filename: str) -> pd.DataFrame:
    """
    Lee un archivo Excel (.xlsx) o CSV (.csv) con detección de codificación.
    Aplica configuraciones específicas definidas en EXCEL_READ_CONFIGS.
    """
    file_stream.seek(0)
    lower = filename.lower()

    # === Excel ===
    if filename.endswith('.xlsx'):
        # 2) Buscar la primera keyword en el nombre
        for kw, params in EXCEL_READ_CONFIGS.items():
            if kw in lower:
                try:
                    return pd.read_excel(file_stream, **params)
                except Exception as e:
                    raise ValueError(f"Error leyendo Excel '{filename}' (config '{kw}'): {e}")
        # 3) Default si no hubo match
        try:
            return pd.read_excel(file_stream, engine='openpyxl')
        except Exception as e:
            raise ValueError(f"Error leyendo Excel '{filename}' (default): {e}")

    # === CSV ===
    elif filename.endswith('.csv'):
        try:
            raw = file_stream.read()
            encoding = chardet.detect(raw).get("encoding", "utf-8")
            file_stream.seek(0)
            return pd.read_csv(file_stream, encoding=encoding)
        except Exception as e:
            raise ValueError(f"Error leyendo CSV '{filename}': {e}")

    else:
        raise ValueError(f"Formato no soportado: '{filename}'")

async def handle_file_upload_generic(
    files: List[UploadFile],
    validator: Callable[[str], str],
    keyword_to_slot: Mapping[str, str],
    required_slots: List[str],
    post_process: Callable[..., Any]
) -> Any:
    """
    - files: lista de UploadFile
    - validator: función file_name → validated_name
    - keyword_to_slot: dict donde clave es keyword en filename validado, valor es nombre de atributo para guardar el DataFrame
    - required_slots: lista de slots obligatorios
    - post_process: función que recibe los DataFrames en orden de required_slots + opcionales, y retorna el resultado final
    """

    # Inicializar slots en None
    slot_data = {slot: None for slot in keyword_to_slot.values()}

    # Procesar cada archivo
    for file in files:
        safe_name = validator(file.filename)
        content = await file.read()
        df = read_file_safely(io.BytesIO(content), safe_name)

        # Buscar a qué slot va
        lower = safe_name.lower()
        for kw, slot in keyword_to_slot.items():
            if kw in safe_name.lower():
                slot_data[slot] = df
                break

    # Verificar faltantes
    missing = [s for s in required_slots if slot_data.get(s) is None]
    if missing:
        raise ValueError(f"Faltan archivos requeridos: {', '.join(missing)}")

    # Llamar al post-proceso con los DataFrames en el orden de required_slots,
    # y además pasar todo slot_data si el post_process lo necesita.
    dfs_required = [slot_data[s] for s in required_slots]

    optional_slots = {
        slot: df
        for slot, df in slot_data.items()
        if slot not in required_slots
    }
    return post_process(*dfs_required, **optional_slots)
