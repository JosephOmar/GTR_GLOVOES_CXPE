import io
import pandas as pd
import chardet
from fastapi import UploadFile
from app.utils.validate_excel_data_kpis import validate_excel_data_kpis
from app.core.data_cleaning_planned_kpis.clean_planned_data import clean_planned_data


def read_file_safely(file_stream: io.BytesIO, filename: str) -> pd.DataFrame:
    """
    Lee un archivo Excel o CSV con manejo de errores y detección automática de codificación.
    """
    file_stream.seek(0)

    if filename.endswith('.xlsx'):
        try:
            if "planned_data" in filename.lower():
                return pd.read_excel(file_stream, sheet_name="DDPP",engine='openpyxl')
            elif "scheduling_ppp" in filename.lower():
                return pd.read_excel(file_stream, header=2, skiprows=3, sheet_name="RESUMEN", engine='openpyxl')
            elif "master_glovo" in filename.lower():
                return pd.read_excel(file_stream, sheet_name="AGENTES_UBY",engine='openpyxl')
            return pd.read_excel(file_stream, engine='openpyxl')
        except Exception as e:
            raise ValueError(f"Error leyendo archivo Excel {filename}: {str(e)}")

    elif filename.endswith('.csv'):
        try:
            raw_data = file_stream.read()
            detected = chardet.detect(raw_data)
            encoding = detected.get("encoding", "utf-8")
            file_stream.seek(0)
            return pd.read_csv(file_stream, encoding=encoding)
        except Exception as e:
            raise ValueError(f"Error leyendo archivo CSV {filename}: {str(e)}")

    else:
        raise ValueError(f"Formato de archivo no soportado: {filename}")


# Función para manejar la subida y procesamiento de los archivos
async def handle_file_upload_planned_data(file1: UploadFile):
    files = [file1]

    file1_data = None
    # file2_data = None
    # file3_data = None
    # file4_data = None
    # file5_data = None

    for file in files:
        new_filename = validate_excel_data_kpis(file.filename)

        file_content = await file.read()
        file_stream = io.BytesIO(file_content)

        try:
            data = read_file_safely(file_stream, new_filename)
        except Exception as e:
            raise ValueError(f"Error procesando {new_filename}: {e}")

        if "planned_data" in new_filename.lower():
            file1_data = data
        elif "scheduling_ppp" in new_filename.lower():
            file2_data = data
        elif "report_kustomer" in new_filename.lower():
            file3_data = data
        elif "master_glovo" in new_filename.lower():
            file4_data = data
        elif "scheduling_ubycall" in new_filename.lower():
            file5_data = data
        else:
            continue  # Ignorar otros archivos

    if file1_data is not None: #and file2_data is not None and file3_data is not None
        planned_data = clean_planned_data(file1_data)

        # worker_data = pd.concat([worker_cx_data, worker_uby_data])
    else:
        raise ValueError("No se encontraron los archivos requeridos para realizar el merge (people_consultation, scheduling_ppp, report_kustomer).")

    return planned_data
