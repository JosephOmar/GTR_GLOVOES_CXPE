import requests
from fastapi import APIRouter, HTTPException, Request

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
