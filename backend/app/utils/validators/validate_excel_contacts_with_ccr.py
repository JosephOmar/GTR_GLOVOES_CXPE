from app.utils.validators.common import validate_and_map_filename

CONTACTS_MAPPING = {
    "contacts_with_ccr": "contacts_with_ccr",
}

def validate_excel_contacts_with_ccr(file_name: str) -> str:
    return validate_and_map_filename(
        file_name=file_name,
        name_mapping=CONTACTS_MAPPING,
        error_detail_no_keyword="El archivo debe tener un nombre válido (contacts_with_ccr)."
    )
