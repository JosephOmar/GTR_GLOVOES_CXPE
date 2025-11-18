import requests
from fastapi import APIRouter, HTTPException, Request
import time

router = APIRouter()

@router.post("/send-to-sheets")
async def send_to_google_sheets(request: Request):
    try:
        data = await request.json()

        google_script_url = "https://script.google.com/macros/s/AKfycbw2rpipc24rY_u8cLz7Gra0odNoIsQvc5lSAZhN3krEhtrDAViPb_noVHLvAtI8qJO_/exec"

        headers = {
            "Content-Type": "application/json"
        }

        response = requests.post(google_script_url, json=data, headers=headers)
        response.raise_for_status()

        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Error al reenviar datos a Google Sheets")


@router.get("/get-from-sheets-vendor")
async def get_from_google_sheets(date: str):
    try:
        google_script_url = f"https://script.google.com/a/macros/service.glovoapp.com/s/AKfycbwvtjSWM_nxoYD5n0m26oA4XNC3k6Pzoe-aNdDNz2YF4eBRMA5rEY0-rSWWUX-u164SxQ/exec?date={date}"
        
        # Realizamos la solicitud GET a la URL del script de Google Apps
        response = requests.get(google_script_url)

        # Verificamos el contenido de la respuesta
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail=f"Error al realizar la solicitud: {response.status_code}")
        
        # Mostrar el contenido de la respuesta
        print("Contenido de la respuesta:", response.text)
        
        # Intentamos decodificar el JSON de la respuesta
        data = response.json()  # Aquí podría fallar si la respuesta no es un JSON válido
        
        # Devolvemos los datos en formato JSON a React
        return data

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise HTTPException(status_code=500, detail="Error al obtener datos de Google Sheets")
    except requests.exceptions.RequestException as req_err:
        print(f"Error during request: {req_err}")
        raise HTTPException(status_code=500, detail="Error en la solicitud a Google Sheets")
    except ValueError as json_err:
        print(f"Error parsing JSON: {json_err}")
        raise HTTPException(status_code=500, detail="Error al parsear la respuesta de Google Sheets")
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="Error inesperado al obtener los datos")


@router.get("/get-from-sheets-customer")
async def get_from_google_sheets(date: str):
    try:
        start_time = time.time()  # Tiempo inicial

        google_script_url = f"https://script.google.com/macros/s/AKfycbwT-mWHbPpMHRhXehZrzeuPblh6R418iP7ApApGC8tkJmIrIN1kIRM-o0PqOPu4I9Fz/exec?date={date}"
        
        # Realizamos la solicitud GET a la URL del script de Google Apps
        print(f"Realizando solicitud GET a la URL: {google_script_url}")
        request_start_time = time.time()  # Tiempo de inicio de la solicitud
        response = requests.get(google_script_url)
        request_end_time = time.time()  # Tiempo de fin de la solicitud

        print(f"Tiempo de solicitud a Google Sheets: {request_end_time - request_start_time:.4f} segundos")

        # Verificamos el contenido de la respuesta
        if response.status_code != 200:
            raise HTTPException(status_code=500, detail=f"Error al realizar la solicitud: {response.status_code}")
        
        # Intentamos decodificar el JSON de la respuesta
        data = response.json()  # Aquí podría fallar si la respuesta no es un JSON válido

        # Calcular el tiempo total desde el inicio hasta la devolución de los datos
        end_time = time.time()  # Tiempo final
        print(f"Tiempo total para obtener los datos: {end_time - start_time:.4f} segundos")
        
        # Devolvemos los datos en formato JSON a React
        return data

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise HTTPException(status_code=500, detail="Error al obtener datos de Google Sheets")
    except requests.exceptions.RequestException as req_err:
        print(f"Error during request: {req_err}")
        raise HTTPException(status_code=500, detail="Error en la solicitud a Google Sheets")
    except ValueError as json_err:
        print(f"Error parsing JSON: {json_err}")
        raise HTTPException(status_code=500, detail="Error al parsear la respuesta de Google Sheets")
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
        raise HTTPException(status_code=500, detail="Error inesperado al obtener los datos")

