import gspread
from google.oauth2.service_account import Credentials
import time

# Define los 'scopes' o permisos que la API necesitará.
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file"
]

# Carga las credenciales desde el archivo JSON que descargaste.
# Asegúrate de que 'credentials.json' esté en la raíz del proyecto.
CREDS = Credentials.from_service_account_file("credentials.json", scopes=SCOPES)
CLIENT = gspread.authorize(CREDS)

# El nombre EXACTO de tu hoja de Google Sheets.
SHEET_NAME = "NombreDeTuHojaDeCalculo"

# --- Implementación del caché ---
CACHE = {
    'datos': None,
    'timestamp': 0
}
CACHE_DURATION_SECONDS = 300  # 5 minutos (300 segundos)

def obtener_datos_certificados():
    """
    Se conecta a la hoja de cálculo y devuelve todos los registros.
    Utiliza un sistema de caché para no llamar a la API repetidamente.
    """
    current_time = time.time()
    
    # 1. Comprueba si el caché es válido (si no ha expirado)
    if CACHE['datos'] and (current_time - CACHE['timestamp'] < CACHE_DURATION_SECONDS):
        print("Cargando datos de certificados desde el CACHÉ.")
        return CACHE['datos']

    # 2. Si el caché no es válido, carga los datos desde Google Sheets
    print("Cargando datos de certificados desde la API de Google Sheets.")
    try:
        spreadsheet = CLIENT.open(SHEET_NAME)
        worksheet = spreadsheet.sheet1
        datos = worksheet.get_all_records()
        
        # 3. Actualiza el caché con los nuevos datos y la hora actual
        CACHE['datos'] = datos
        CACHE['timestamp'] = current_time
        
        return datos
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Error: No se encontró la hoja de cálculo '{SHEET_NAME}'.")
        return []
    except Exception as e:
        print(f"Ocurrió un error inesperado al cargar desde la API: {e}")
        return []