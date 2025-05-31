import io
import pandas as pd
import chardet
from fastapi import UploadFile
from app.utils.validate_excel_real_data_view import validate_excel_real_data_view
from app.core.data_cleaning_real_data_view.clean_real_data_view import merge_data_view


def read_file_safely(file_stream: io.BytesIO, filename: str) -> pd.DataFrame:
    """
    Lee un archivo Excel o CSV con manejo de errores y detección automática de codificación.
    """
    file_stream.seek(0)

    if filename.endswith('.xlsx'):
        try:
            if "planned_data" in filename.lower():
                return pd.read_excel(file_stream, sheet_name="DDPP", engine='openpyxl')
            elif "scheduling_ppp" in filename.lower():
                return pd.read_excel(file_stream, header=2, skiprows=3, sheet_name="RESUMEN", engine='openpyxl')
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


async def handle_file_upload_real_data_view(file1: UploadFile, file2: UploadFile, file3: UploadFile, file4: UploadFile, file5: UploadFile, file6: UploadFile, file7: UploadFile, file8: UploadFile):
    files = [file1, file2, file3, file4, file5, file6, file7, file8]

    file1_data = None
    file2_data = None
    file3_data = None
    file4_data = None
    file5_data = None
    file6_data = None
    file7_data = None
    file8_data = None

    print("📥 Procesando archivos subidos:")
    
    for file in files:
        print(f"🔸 Archivo recibido: {file.filename}")

        new_filename = validate_excel_real_data_view(file.filename)
        print(f"➡️  Nombre validado/renombrado: {new_filename}")

        file_content = await file.read()
        file_stream = io.BytesIO(file_content)

        try:
            data = read_file_safely(file_stream, new_filename)
        except Exception as e:
            raise ValueError(f"❌ Error procesando {new_filename}: {e}")

        if "planned_data" in new_filename.lower():
            print("✅ Asignado a file1_data")
            file1_data = data
        elif "assembled_data_chat" in new_filename.lower():
            print("✅ Asignado a file2_data")
            file2_data = data
        elif "assembled_data_call" in new_filename.lower():
            print("✅ Asignado a file3_data")
            file3_data = data
        elif "kustomer_data_cr_range" in new_filename.lower():
            print("✅ Asignado a file4_data")
            file4_data = data
        elif "kustomer_data_cr_total" in new_filename.lower():
            print("✅ Asignado a file5_data")
            file5_data = data
        elif "kustomer_data_rd_range" in new_filename.lower():
            print("✅ Asignado a file6_data")
            file6_data = data
        elif "kustomer_data_rd_total" in new_filename.lower():
            print("✅ Asignado a file7_data")
            file7_data = data
        elif "kustomer_data_real_agents" in new_filename.lower():
            print("✅ Asignado a file8_data")
            file8_data = data
        else:
            print("⚠️  Archivo ignorado (nombre no reconocido)")

    # Log final para verificar cuál faltó
    missing = []
    if file1_data is None: missing.append("planned_data")
    if file2_data is None: missing.append("assembled_data_chat")
    if file3_data is None: missing.append("assembled_data_call")
    if file4_data is None: missing.append("kustomer_data_cr_range")
    if file5_data is None: missing.append("kustomer_data_cr_total")
    if file6_data is None: missing.append("kustomer_data_rd_range")
    if file7_data is None: missing.append("kustomer_data_rd_total")
    if file8_data is None: missing.append("kustomer_data_real_agents")

    if missing:
        print("🚫 Archivos faltantes:", ", ".join(missing))
        raise ValueError("Faltan archivos requeridos para realizar el merge de real_data_view.")

    print("✅ Todos los archivos requeridos fueron cargados correctamente.")
    data = merge_data_view(file1_data, file2_data, file3_data, file4_data, file5_data, file6_data, file7_data, file8_data)

    return data
