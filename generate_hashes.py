# generate_hashes.py
from werkzeug.security import generate_password_hash

# --- Lista de contraseñas a encriptar ---
passwords = {
    'admin_pass': 'centro-admin',
    'equipo_pass': 'centro'
}

print("--- COPIA Y PEGA ESTOS HASHES EN TU ARCHIVO routes.py ---")

for name, plain_password in passwords.items():
    # Generamos el hash para cada contraseña
    hashed_password = generate_password_hash(plain_password)
    print(f"\n# Para la contraseña: '{plain_password}'")
    print(f"'{name}_hash': '{hashed_password}',")

print("\n--- PROCESO TERMINADO ---")