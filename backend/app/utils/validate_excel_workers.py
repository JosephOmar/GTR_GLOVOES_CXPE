import re
from fastapi import HTTPException

# Función para validar si un archivo es Excel (.xlsx) o CSV y si contiene parte del nombre esperado
def validate_excel_workers(file_name: str) -> str:
    # Diccionario para mapear los nombres a su formato deseado
    name_mapping = {
        "ConsultaPersonal": "people_consultation",
        "Programación Glovo": "scheduling_ppp",
        "report": "report_kustomer",
        "Maestro_Glovo": "master_glovo",
        "Ubycall": "scheduling_ubycall",
        "Reporte Descarga Horario": "schedule_concentrix"
    }

    # Verificar si alguna de las palabras clave está en el nombre del archivo
    if not any(keyword in file_name for keyword in name_mapping):
        raise HTTPException(status_code=400, detail="El archivo debe tener un nombre válido (Consulta Personal, Programacion Glovo, o Report).")

    # Validar extensión de archivo (solo .xlsx y .csv permitidos)
    if not (file_name.endswith('.xlsx') or file_name.endswith('.csv')):
        raise HTTPException(status_code=400, detail="El archivo debe ser un archivo Excel (.xlsx) o CSV (.csv).")
    
    # Cambiar el nombre del archivo según la regla definida
    for keyword, new_name in name_mapping.items():
        if keyword in file_name:
            # Sustituir las palabras clave por el nuevo nombre
            file_name = file_name.replace(keyword, new_name)
            break

    # Asegurarse de que el archivo no tenga espacios en blanco o caracteres no deseados
    file_name = file_name.replace(" ", "_")  # Reemplazamos espacios por guiones bajos

    return file_name
