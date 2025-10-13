# test_sheets.py

import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURACIÓN ---
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file"
]

# ¡¡¡CAMBIO AQUÍ!!!
# Ya no usaremos el nombre, usaremos el ID
SHEET_ID = "1WxuhyGTskTmYBcMmFd9aN8JN0tfBV7wtWF78K7JL-cw"  # <-- Pega aquí el ID de la URL de tu hoja "brainrot"

CREDS_FILE = "credentials.json"

# --- CÓDIGO DE PRUEBA ---
print("--- Iniciando prueba de conexión con Google Sheets ---")

try:
    print(f"1. Cargando credenciales desde '{CREDS_FILE}'...")
    creds = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    client = gspread.authorize(creds)
    print("   -> Autenticación exitosa.")

    # ¡¡¡CAMBIO AQUÍ!!!
    # Usamos el método open_by_key que es más directo
    print(f"2. Intentando abrir la hoja de cálculo por su ID: '{SHEET_ID}'...")
    spreadsheet = client.open_by_key(SHEET_ID)
    print("   -> ¡ÉXITO! Se encontró y abrió la hoja de cálculo.")

    print("3. Leyendo la primera hoja de trabajo...")
    worksheet = spreadsheet.sheet1
    records = worksheet.get_all_records()
    print(f"   -> Se encontraron {len(records)} registros.")
    
    if records:
        print("\n--- ¡PRUEBA SUPERADA! ---")
        print(records[:2])
    else:
        print("\n--- PRUEBA SUPERADA, PERO LA HOJA ESTÁ VACÍA ---")

except gspread.exceptions.SpreadsheetNotFound:
    print(f"\n--- ERROR DE PERMISOS DEFINITIVO ---")
    print("La hoja de cálculo con ese ID no se pudo encontrar o no tienes permiso.")
    print("Esto confirma que el problema está en una restricción a nivel de tu cuenta de Google o del Proyecto de Google Cloud.")

except Exception as e:
    print(f"\n--- OCURRIÓ OTRO ERROR ---")
    print(e)