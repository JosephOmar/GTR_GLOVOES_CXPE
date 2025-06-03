from fastapi import HTTPException

def validate_excel_real_data_view(file_name: str) -> str:
    name_mapping = {
        "planned_data": "planned_data",
        "assembled_chat": "assembled_data_chat",
        "assembled_call": "assembled_data_call",
        "looker_customer":"looker_data_customer",
        "looker_rider":"looker_data_rider",
        "sat_customer_total": "kustomer_data_cr_total",
        "sat_customer": "kustomer_data_cr_range",
        "sat_rider_total": "kustomer_data_rd_total",
        "sat_rider": "kustomer_data_rd_range",
        "real_agents": "kustomer_data_real_agents"
    }

    # Validar que exista algún keyword
    if not any(keyword in file_name for keyword in name_mapping):
        raise HTTPException(status_code=400, detail="El archivo debe tener un nombre válido.")

    if not (file_name.endswith('.xlsx') or file_name.endswith('.csv')):
        raise HTTPException(status_code=400, detail="El archivo debe ser un Excel (.xlsx) o CSV (.csv)")

    # ✅ Reordenar para procesar claves más largas primero (evita reemplazos parciales incorrectos)
    for keyword in sorted(name_mapping.keys(), key=len, reverse=True):
        if keyword in file_name:
            file_name = file_name.replace(keyword, name_mapping[keyword])
            break

    return file_name.replace(" ", "_")
