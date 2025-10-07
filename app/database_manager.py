"""
Módulo de Gestión de Base de Datos
---------------------------------
Contiene las funciones para interactuar con la base de datos MySQL.
La conexión, las operaciones CRUD para clientes y pagos, la generación de
reportes y el registro de auditoría.
"""

# --- Importaciones ---
import io
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import pandas as pd

# --- Configuración ---
DB_CONFIG = {
    "host": "localhost",
    "database": "registro_app_db",
    "user": "root",
    "password": "pho3nix241236!",
}

# --- Manejo de Conexión ---
def get_connection():
    """
    Establece y devuelve una nueva conexión a la base de datos.
    
    Returns:
        mysql.connector.connection: Objeto de conexión a la base de datos.
    """
    return mysql.connector.connect(**DB_CONFIG)

# --- Funciones de Estadísticas para el Dashboard ---

def obtener_estadisticas_dashboard():
    """
    Obtiene estadísticas clave para mostrar en el panel principal.
    
    Returns:
        dict: Un diccionario con registros_hoy, ingresos_hoy, e ingresos_mes.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        stats = {
            'registros_hoy': 0,
            'ingresos_hoy': 0.0,
            'ingresos_mes': 0.0
        }

        # 1. Conteo de registros de hoy
        sql_hoy = "SELECT COUNT(*) as total FROM pagos WHERE DATE(fecha) = CURDATE()"
        cursor.execute(sql_hoy)
        resultado = cursor.fetchone()
        if resultado and resultado['total']:
            stats['registros_hoy'] = resultado['total']

        # 2. Suma de ingresos de hoy
        sql_ingresos_hoy = "SELECT SUM(cuota) as total FROM pagos WHERE DATE(fecha) = CURDATE()"
        cursor.execute(sql_ingresos_hoy)
        resultado = cursor.fetchone()
        if resultado and resultado['total']:
            stats['ingresos_hoy'] = resultado['total']

        # 3. Suma de ingresos del mes actual
        sql_ingresos_mes = """SELECT SUM(cuota) as total FROM pagos 
                              WHERE YEAR(fecha) = YEAR(CURDATE()) AND MONTH(fecha) = MONTH(CURDATE())"""
        cursor.execute(sql_ingresos_mes)
        resultado = cursor.fetchone()
        if resultado and resultado['total']:
            stats['ingresos_mes'] = resultado['total']
            
        return stats
    except Error as e:
        print(f"ERROR EN BD (obtener_estadisticas_dashboard): {e}")
        # Devuelve stats en cero si hay un error
        return {'registros_hoy': 0, 'ingresos_hoy': 0, 'ingresos_mes': 0}
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def obtener_ultimos_pagos(limit=5):
    """
    Obtiene los últimos registros de pago para mostrar en el dashboard.
    
    Args:
        limit (int): El número de registros a obtener.
    
    Returns:
        list: Una lista de tuplas con los últimos pagos.
    """
    # Esta función es muy similar a buscar_pagos_completos, pero sin búsqueda
    # y con un límite. Podemos reutilizar esa lógica.
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = """
            SELECT p.id, p.fecha, c.nombre, c.celular, p.especialidad, p.modalidad,
                   p.cuota, p.tipo_de_cuota, p.banco, p.destino, p.numero_operacion,
                   c.dni, c.correo, c.genero, p.asesor
            FROM pagos p JOIN clientes c ON p.cliente_id = c.id
            ORDER BY p.id DESC
            LIMIT %s
        """
        cursor.execute(sql, (limit,))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (obtener_ultimos_pagos): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# --- Gestión de Clientes y Pagos (CRUD) ---

#Función para buscar o crear cliente
def buscar_o_crear_cliente(data):
    """
    Busca un cliente por DNI. 
    - Si existe y está inactivo, lo reactiva.
    - Si no existe, lo crea.
    Devuelve el ID del cliente.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        
        # 1. Buscar si el cliente ya existe por su DNI, sin importar su estado
        sql_buscar = "SELECT id, estado FROM clientes WHERE dni = %s"
        cursor.execute(sql_buscar, (data.get('dni'),))
        cliente_existente = cursor.fetchone()
        
        if cliente_existente:
            # --- CLIENTE ENCONTRADO ---
            cliente_id = cliente_existente['id']
            
            # 2. Si estaba inactivo, lo reactivamos
            if cliente_existente['estado'] == 'inactivo':
                print(f"Reactivando cliente inactivo con ID: {cliente_id}")
                sql_reactivar = "UPDATE clientes SET estado = 'activo' WHERE id = %s"
                # Usamos un nuevo cursor para esta operación de escritura
                update_cursor = conn.cursor()
                update_cursor.execute(sql_reactivar, (cliente_id,))
                conn.commit()
                update_cursor.close()
            
            return cliente_id
        else:
            # --- CLIENTE NO ENCONTRADO, SE CREA UNO NUEVO ---
            sql_crear = """INSERT INTO clientes (nombre, dni, correo, celular, genero, estado)
                            VALUES (%s, %s, %s, %s, %s, 'activo')""" # Se crea como activo
            cliente_tuple = (
                data.get('cliente'), data.get('dni'), data.get('correo'),
                data.get('celular'), data.get('genero')
            )
            cursor.execute(sql_crear, cliente_tuple)
            conn.commit()
            return cursor.lastrowid
            
    except Error as e:
        print(f"ERROR EN BD (buscar_o_crear_cliente): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexión cerrada.")
#Función para crear pago
def crear_pago(cliente_id, data):
    """
    Crea un nuevo registro en la tabla `pagos`.

    Args:
        cliente_id (int): El ID del cliente al que se asocia el pago.
        data (dict): Diccionario con los datos del formulario del pago.

    Returns:
        int: El ID del nuevo pago creado.

    Raises:
        mysql.connector.Error: Si ocurre un error en la base de datos.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = """INSERT INTO pagos (cliente_id, fecha, cuota, tipo_de_cuota, banco, destino, 
                numero_operacion, especialidad, modalidad, asesor)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        pago_tuple = (
            cliente_id, data.get('fecha'), data.get('cuota'), data.get('tipo_cuota'),
            data.get('banco'), data.get('destino'), data.get('numero_operacion'),
            data.get('especialidad'), data.get('modalidad'), data.get('asesor')
        )
        cursor.execute(sql, pago_tuple)
        conn.commit()
        return cursor.lastrowid
    except Error as e:
        print(f"ERROR EN BD (crear_pago): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#Función para buscar pagos con datos de cliente
def buscar_pagos_completos(query):
    """Busca pagos y une la información del cliente, incluyendo el ID del cliente."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        # ¡AÑADIMOS c.id AL SELECT!
        sql = """
            SELECT 
                p.id, p.fecha, c.nombre, c.celular, p.especialidad, p.modalidad,
                p.cuota, p.tipo_de_cuota, p.banco, p.destino, p.numero_operacion,
                c.dni, c.correo, c.genero, p.asesor, c.id as cliente_id
            FROM pagos p
            JOIN clientes c ON p.cliente_id = c.id
            WHERE c.dni LIKE %s OR c.nombre LIKE %s
            ORDER BY p.id DESC
        """
        search_term = f"%{query}%"
        cursor.execute(sql, (search_term, search_term))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (buscar_pagos_completos): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#Función para obtener pago por ID
def obtener_pago_por_id(pago_id):
    """
    Obtiene un registro de pago y sus datos de cliente asociados por el ID del pago.

    Args:
        pago_id (int): El ID del pago a buscar.

    Returns:
        dict: Un diccionario con los datos del pago y del cliente, o None si no se encuentra o hay un error.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """
            SELECT p.*, c.nombre as cliente, c.dni, c.correo, c.celular, c.genero
            FROM pagos p JOIN clientes c ON p.cliente_id = c.id
            WHERE p.id = %s
        """
        cursor.execute(sql, (pago_id,))
        return cursor.fetchone()
    except Error as e:
        print(f"ERROR EN BD (obtener_pago_por_id): {e}")
        return None
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#Función para actualizar pago
def actualizar_pago(pago_id, form_data):
    """
    Actualiza los campos de un registro de pago existente.

    Args:
        pago_id (int): El ID del pago a actualizar.
        form_data (dict): Diccionario con los nuevos datos del formulario.

    Returns:
        int: El número de filas afectadas (normalmente 1).

    Raises:
        mysql.connector.Error: Si ocurre un error en la base de datos.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        campos_pago = [
            "fecha", "cuota", "tipo_de_cuota", "banco", "destino",
            "numero_operacion", "especialidad", "modalidad", "asesor"
        ]
        set_clause = ", ".join([f"{campo} = %s" for campo in campos_pago])
        sql = f"UPDATE pagos SET {set_clause} WHERE id = %s"
        
        form_data_copy = form_data.copy()
        if 'num_operacion' in form_data_copy:
            form_data_copy['numero_operacion'] = form_data_copy.pop('num_operacion')

        valores = [form_data_copy.get(campo) for campo in campos_pago] + [pago_id]
        cursor.execute(sql, tuple(valores))
        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (actualizar_pago): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# Función para eliminar pago
def eliminar_pago(pago_id):
    """
    Elimina un registro de pago de la base de datos.

    Args:
        pago_id (int): El ID del pago a eliminar.

    Returns:
        int: El número de filas afectadas (normalmente 1).

    Raises:
        mysql.connector.Error: Si ocurre un error en la base de datos.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = "DELETE FROM pagos WHERE id = %s"
        cursor.execute(sql, (pago_id,))
        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (eliminar_pago): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# --- Reportes y Exportación ---

#Función para generar reporte de asesores
def generar_reporte_asesores_db(start_date_str=None, end_date_str=None):
    """
    Genera un reporte de ventas agrupado por asesor con filtro de fecha.

    Args:
        start_date_str (str, optional): Fecha de inicio en formato 'YYYY-MM-DD'.
        end_date_str (str, optional): Fecha de fin en formato 'YYYY-MM-DD'.

    Returns:
        list: Lista de diccionarios con los resultados del reporte.
            Devuelve una lista vacía si hay un error.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """SELECT asesor, COUNT(*) as registros_asesor, SUM(cuota) as total_asesor 
                FROM pagos"""
        params, where_clauses = [], []
        if start_date_str:
            where_clauses.append("fecha >= %s")
            params.append(start_date_str)
        if end_date_str:
            where_clauses.append("fecha <= %s")
            params.append(end_date_str)
        if where_clauses:
            sql += " WHERE " + " AND ".join(where_clauses)
        sql += " GROUP BY asesor ORDER BY total_asesor DESC"
        cursor.execute(sql, tuple(params))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (generar_reporte_asesores_db): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#Función para generar Excel dinámico
def generar_excel_dinamico(headers):
    """
    Crea un archivo Excel en memoria con todos los registros,
    manejando fechas inválidas de forma robusta.
    """
    conn = None
    try:
        conn = get_connection()
        sql = """
            SELECT p.id, p.fecha, c.nombre, c.celular, p.especialidad, p.modalidad,
                   p.cuota, p.tipo_de_cuota, p.banco, p.destino, p.numero_operacion,
                   c.dni, c.correo, c.genero, p.asesor
            FROM pagos p JOIN clientes c ON p.cliente_id = c.id
            ORDER BY p.id ASC
        """
        df = pd.read_sql(sql, conn)
        df.columns = ["ID"] + headers
        
        # --- LÓGICA DE FECHA MEJORADA ---
        # 1. Convierte las fechas. Si una es inválida, la convierte en 'NaT' (Not a Time)
        df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce')
        
        # 2. Formatea solo las fechas que son válidas.
        # Las fechas inválidas (NaT) se convertirán en un valor nulo (NaN).
        df['FECHA'] = df['FECHA'].dt.strftime('%Y-%m-%d')

        # 3. Reemplaza cualquier valor nulo restante con un string vacío para un Excel limpio.
        df.fillna('', inplace=True)
        
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Registros')
        output.seek(0)
        return output
    except Error as e:
        print(f"ERROR EN BD (generar_excel_dinamico): {e}")
        return None
    finally:
        if conn and conn.is_connected():
            conn.close()
            
            
# --- Módulo de Auditoría ---

#Función para registrar auditoría
def registrar_auditoria(usuario, accion, ip, tabla=None, reg_id=None, detalles=None):
    """
    Inserta un nuevo registro en la tabla de auditoría.
    Esta función no propaga errores para no detener la operación principal.

    Args:
        usuario (str): Nombre del usuario que realiza la acción.
        accion (str): Descripción de la acción (ej: 'CREAR_PAGO').
        ip (str): Dirección IP de origen de la petición.
        tabla (str, optional): Nombre de la tabla afectada.
        reg_id (int, optional): ID del registro afectado.
        detalles (str, optional): Información adicional sobre el evento.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = """INSERT INTO auditoria_accesos 
                (timestamp, usuario_app, accion, tabla_afectada, registro_id, detalles, ip_origen)
                VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        datos = (datetime.now(), usuario, accion, tabla, reg_id, detalles, ip)
        cursor.execute(sql, datos)
        conn.commit()
    except Error as e:
        print(f"ERROR CRÍTICO AL REGISTRAR AUDITORÍA: {e}")
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

#Función para leer log de auditoría            
def leer_log_auditoria():
    """
    Lee todos los registros de la tabla de auditoría ordenados por fecha.

    Returns:
        list: Lista de diccionarios con los logs de auditoría.
            Devuelve una lista vacía si hay un error.
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = "SELECT * FROM auditoria_accesos ORDER BY timestamp DESC"
        cursor.execute(sql)
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (leer_log_auditoria): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            
            
# Función para cambiar estado de cliente            
def cambiar_estado_cliente(cliente_id, nuevo_estado):
    """
    Cambia el estado de un cliente a 'activo' o 'inactivo'.

    Args:
        cliente_id (int): El ID del cliente a modificar.
        nuevo_estado (str): El nuevo estado ('activo' o 'inactivo').
    """
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = "UPDATE clientes SET estado = %s WHERE id = %s"
        cursor.execute(sql, (nuevo_estado, cliente_id))
        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (cambiar_estado_cliente): {e}")
        raise e
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()
            
# Función para obtener cliente por ID            
def obtener_cliente_por_id(cliente_id):
    """Obtiene los datos de un único cliente por su ID."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = "SELECT * FROM clientes WHERE id = %s"
        cursor.execute(sql, (cliente_id,))
        return cursor.fetchone()
    except Error as e:
        print(f"ERROR EN BD (obtener_cliente_por_id): {e}")
        return None
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

# Función para obtener pagos por cliente
def obtener_pagos_por_cliente(cliente_id):
    """Obtiene todos los pagos asociados a un ID de cliente."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = "SELECT * FROM pagos WHERE cliente_id = %s ORDER BY fecha DESC"
        cursor.execute(sql, (cliente_id,))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (obtener_pagos_por_cliente): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()