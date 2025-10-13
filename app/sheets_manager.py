# app/sheets_manager.py
import gspread
from google.oauth2.service_account import Credentials
import time

# --- CONFIGURACIÓN ---
SCOPES = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file"]
CREDS_FILE = "credentials.json"

# --- IDs y Nombres de las Hojas ---
CERTIFICADOS_SHEET_ID = "1WxuhyGTskTmYBcMmFd9aN8JN0tfBV7wtWF78K7JL-cw"
CERTIFICADOS_WORKSHEET_NAME = "Form Responses 1"

DIPLOMADOS_SHEET_ID = "1xWgTYcMgjQYf6ZDlhqszoALWyQICEBx5RBKX0OcOICc"
DIPLOMADOS_WORKSHEET_NAME = "Form Responses 1" # O el nombre de la pestaña de diplomados

# --- Carga de credenciales ---
try:
    CREDS = Credentials.from_service_account_file(CREDS_FILE, scopes=SCOPES)
    CLIENT = gspread.authorize(CREDS)
    print("-> Cliente de Google Sheets autenticado correctamente.")
except Exception as e:
    print(f"ERROR CRÍTICO al cargar las credenciales: {e}")
    CLIENT = None

# --- Implementación de Cachés (uno para cada sección) ---
CERTIFICADOS_CACHE = {'datos': None, 'timestamp': 0}
DIPLOMADOS_CACHE = {'datos': None, 'timestamp': 0}
CACHE_DURATION_SECONDS = 300

# --- FUNCIONES GENÉRICAS ---
def _obtener_datos_generico(sheet_id, worksheet_name, cache):
    if not CLIENT: return []
    current_time = time.time()
    
    if cache['datos'] and (current_time - cache['timestamp'] < CACHE_DURATION_SECONDS):
        print(f"Cargando datos de '{worksheet_name}' desde el CACHÉ.")
        return cache['datos']

    print(f"Cargando datos de '{worksheet_name}' desde la API.")
    try:
        spreadsheet = CLIENT.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        all_values = worksheet.get_all_values()
        if not all_values: return []

        headers = all_values[0]
        data_rows = all_values[1:]
        datos = []
        for i, row in enumerate(data_rows, start=2):
            record = {'row_id': i}
            for j, header in enumerate(headers):
                if header and header.strip():
                    if j < len(row): record[header] = row[j]
                    else: record[header] = ""
            if any(str(val).strip() for h, val in record.items() if h != 'row_id'):
                datos.append(record)
        
        cache['datos'] = datos
        cache['timestamp'] = current_time
        return datos
    except Exception as e:
        print(f"Ocurrió un error al leer la API para la hoja '{worksheet_name}': {e}")
        return []

def _actualizar_registro_generico(sheet_id, worksheet_name, row_id, data, cache):
    if not CLIENT: return
    try:
        spreadsheet = CLIENT.open_by_key(sheet_id)
        worksheet = spreadsheet.worksheet(worksheet_name)
        headers = worksheet.row_values(1)
        update_values = [data.get(header, "") for header in headers]
        worksheet.update(f'A{row_id}', [update_values])
        cache['datos'] = None # Limpiar caché
        cache['timestamp'] = 0
        print(f"Fila {row_id} de '{worksheet_name}' actualizada.")
    except Exception as e:
        print(f"Error al actualizar la fila {row_id} en '{worksheet_name}': {e}")


# --- FUNCIONES ESPECÍFICAS (las que usará routes.py) ---
def obtener_datos_certificados():
    return _obtener_datos_generico(CERTIFICADOS_SHEET_ID, CERTIFICADOS_WORKSHEET_NAME, CERTIFICADOS_CACHE)

def actualizar_certificado(row_id, data):
    _actualizar_registro_generico(CERTIFICADOS_SHEET_ID, CERTIFICADOS_WORKSHEET_NAME, row_id, data, CERTIFICADOS_CACHE)

def obtener_datos_diplomados():
    return _obtener_datos_generico(DIPLOMADOS_SHEET_ID, DIPLOMADOS_WORKSHEET_NAME, DIPLOMADOS_CACHE)

def actualizar_diplomado(row_id, data):
    _actualizar_registro_generico(DIPLOMADOS_SHEET_ID, DIPLOMADOS_WORKSHEET_NAME, row_id, data, DIPLOMADOS_CACHE)