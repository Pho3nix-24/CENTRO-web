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
import pytz
import mysql.connector
from mysql.connector import Error
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os

# --- Configuración ---
DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "database": os.environ.get("DB_NAME", "registro_app_db"),
    "user": os.environ.get("DB_USER", "root"),
    "password": os.environ.get("DB_PASSWORD", "------------------------"),
}


# --- Manejo de Conexión ---
def get_connection():
    """
    Establece y devuelve una nueva conexión a la base de datos.
    """
    return mysql.connector.connect(**DB_CONFIG)


# --- Funciones de Estadísticas para el Dashboard ---

# ================= SECCIÓN METAS DINÁMICAS =================


def obtener_metas():
    """Obtiene los objetivos configurados."""
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        # Asegúrate que el nombre sea metas_config
        cursor.execute(
            "SELECT meta_inscritos, meta_dinero FROM metas_config WHERE id = 1"
        )
        res = cursor.fetchone()
        return res if res else {"meta_inscritos": 0, "meta_dinero": 0.0}
    except Exception as e:
        print(f"Error al obtener metas: {e}")
        return {"meta_inscritos": 0, "meta_dinero": 0.0}
    finally:
        conn.close()


def actualizar_metas(inscritos):
    """Guarda únicamente la nueva meta de inscritos."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Cambiado a un solo parámetro para coincidir con tu ruta
        cursor.execute(
            "UPDATE metas_config SET meta_inscritos = %s WHERE id = 1", (inscritos,)
        )
        conn.commit()
        print(f"Meta actualizada en DB a: {inscritos}")  # Para tu depuración
    except Exception as e:
        print(f"Error al actualizar metas: {e}")
    finally:
        conn.close()


def obtener_estadisticas_dashboard():
    """Calcula métricas financieras y obtiene metas de inscritos usando la hora local de Perú."""
    conn = None
    # Configuración de zona horaria local
    tz_peru = pytz.timezone("America/Lima")
    hoy_peru = datetime.now(tz_peru).date()
    inicio_mes = hoy_peru.replace(day=1)

    # 1. Obtenemos la meta de inscritos desde la nueva tabla
    metas = obtener_metas()

    # Estructura base de estadísticas (Sin estimado_mensual automático)
    stats = {
        "ingresos_hoy": 0.0,
        "ingresos_semana": 0.0,
        "ingresos_mes": 0.0,
        "meta_inscritos": metas.get("meta_inscritos", 0),  # Meta manual
        "grafico_especialidades": {"labels": [], "data": []},
        "grafico_semanal": {
            "labels": ["Lun", "Mar", "Mie", "Jue", "Vie", "Sab", "Dom"],
            "data": [0] * 7,
        },
    }

    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # 1. Ingresos Hoy
        cursor.execute(
            "SELECT SUM(cuota) as total FROM pagos WHERE DATE(fecha) = %s", (hoy_peru,)
        )
        res_hoy = cursor.fetchone()
        stats["ingresos_hoy"] = float(res_hoy["total"] or 0.0)

        # 2. Ingresos Semana
        cursor.execute(
            "SELECT SUM(cuota) as total FROM pagos WHERE fecha >= DATE_SUB(%s, INTERVAL 7 DAY)",
            (hoy_peru,),
        )
        stats["ingresos_semana"] = float(cursor.fetchone()["total"] or 0.0)

        # 3. Ingresos Mes
        cursor.execute(
            "SELECT SUM(cuota) as total FROM pagos WHERE fecha >= %s", (inicio_mes,)
        )
        stats["ingresos_mes"] = float(cursor.fetchone()["total"] or 0.0)

        # 4. Datos para Gráfico de Especialidades
        cursor.execute("""
            SELECT especialidad, COUNT(*) as cantidad 
            FROM pagos 
            WHERE especialidad IS NOT NULL AND especialidad != ''
            GROUP BY especialidad 
            ORDER BY cantidad DESC LIMIT 5
        """)
        for row in cursor.fetchall():
            stats["grafico_especialidades"]["labels"].append(row["especialidad"])
            stats["grafico_especialidades"]["data"].append(row["cantidad"])

        # 5. Datos para Gráfico Semanal
        cursor.execute(
            """
            SELECT DAYOFWEEK(fecha) as dia, SUM(cuota) as total 
            FROM pagos 
            WHERE YEARWEEK(fecha, 1) = YEARWEEK(%s, 1)
            GROUP BY dia
        """,
            (hoy_peru,),
        )

        for row in cursor.fetchall():
            idx = (row["dia"] + 5) % 7
            stats["grafico_semanal"]["data"][idx] = float(row["total"])

        return stats
    except Exception as e:
        print(f"Error en estadísticas: {e}")
        return stats
    finally:
        if conn:
            conn.close()


# Función para obtener los últimos pagos realizados, mostrando información relevante del cliente y el pago, ordenados por fecha descendente.
def obtener_ultimos_pagos(limit=5):
    """
    Obtiene los últimos registros de pago para mostrar en el dashboard.
    """
    conn = None
    cursor = None
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
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


# --- Gestión de Clientes y Pagos (CRUD) ---


def buscar_o_crear_cliente(data):
    """
    Busca un cliente por DNI.
    - Si existe y está inactivo o potencial, lo activa y actualiza sus datos.
    - Si no existe, lo crea como 'activo' (porque está haciendo un pago).
    Devuelve el ID del cliente.
    """
    conn = None
    cursor = None
    update_cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        sql_buscar = "SELECT id, estado FROM clientes WHERE dni = %s"
        cursor.execute(sql_buscar, (data.get("dni"),))
        cliente_existente = cursor.fetchone()

        if cliente_existente:
            cliente_id = cliente_existente["id"]
            if cliente_existente["estado"] != "activo":
                print(
                    f"Actualizando estado del cliente a 'activo' con ID: {cliente_id}"
                )
                update_cursor = conn.cursor()
                sql_actualizar = """
                    UPDATE clientes SET 
                    nombre = %s, correo = %s, celular = %s, genero = %s, estado = 'activo'
                    WHERE id = %s
                """
                datos_actualizar = (
                    data.get("cliente"),
                    data.get("correo"),
                    data.get("celular"),
                    data.get("genero"),
                    cliente_id,
                )
                update_cursor.execute(sql_actualizar, datos_actualizar)
                conn.commit()
            return cliente_id
        else:
            # Si el cliente no existe, se crea directamente como 'activo'
            sql_crear = """INSERT INTO clientes (nombre, dni, correo, celular, genero, estado)
                           VALUES (%s, %s, %s, %s, %s, 'activo')"""
            cliente_tuple = (
                data.get("cliente"),
                data.get("dni"),
                data.get("correo"),
                data.get("celular"),
                data.get("genero"),
            )
            cursor.execute(sql_crear, cliente_tuple)
            conn.commit()
            return cursor.lastrowid

    except Error as e:
        print(f"ERROR EN BD (buscar_o_crear_cliente): {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if update_cursor:
            update_cursor.close()
        if conn and conn.is_connected():
            conn.close()


def crear_pago(cliente_id, data):
    """Crea un registro en la tabla `pagos` y actualiza el estado del cliente."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 1. SQL con el orden exacto de tu base de datos limpia
        sql_pago = """
            INSERT INTO pagos (
                cliente_id, fecha, especialidad, modalidad, cuota, 
                tipo_de_cuota, banco, destino, numero_operacion, asesor,
                monto_total_diplomado, numero_cuota, proxima_fecha_pago
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # 2. Preparación segura de datos numéricos
        monto_total = float(data.get("monto_total_diplomado") or 0.0)
        nro_cuota = int(data.get("numero_cuota") if data.get("numero_cuota") is not None else 1)
        prox_fecha = data.get("proxima_fecha_pago") if data.get("proxima_fecha_pago") else None

        # 3. TUPLA CORREGIDA: Debe seguir EXACTAMENTE el orden del INSERT arriba
        pago_tuple = (
            cliente_id,
            data.get("fecha"),           # fecha
            data.get("especialidad"),    # especialidad
            data.get("modalidad"),       # modalidad
            data.get("cuota"),           # cuota (monto pagado hoy)
            data.get("tipo_cuota"),      # tipo_de_cuota
            data.get("banco"),           # banco
            data.get("destino"),         # destino
            data.get("numero_operacion"),# numero_operacion
            data.get("asesor"),          # asesor
            monto_total,                 # monto_total_diplomado
            nro_cuota,                   # numero_cuota
            prox_fecha                   # proxima_fecha_pago
        )

        # 4. Ejecución (Cambiado 'sql' por 'sql_pago')
        cursor.execute(sql_pago, pago_tuple)

        # --- LÓGICA DE ESTADO AUTOMÁTICA ---
        # 0 en numero_cuota significa pago completo
        nuevo_estado = "FINALIZADO" if nro_cuota == 0 else "AL DIA"

        sql_update_cliente = "UPDATE clientes SET estado_pago = %s WHERE id = %s"
        cursor.execute(sql_update_cliente, (nuevo_estado, cliente_id))

        conn.commit()
        return cursor.lastrowid

    except Exception as e:
        print(f"ERROR EN BD (crear_pago): {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


# Función para reemplazar a buscar_pagos() y mostrar más información relevante en la tabla de pagos, como el estado de deuda del cliente, el monto total del diplomado, el número de cuota y la próxima fecha de pago.
def buscar_pagos_completos(query="", fecha_vence=None, especialidad=None, solo_inactivos=False):
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 1. Base de la consulta con las 21 columnas exactas
        sql = """
            SELECT 
                p.id, p.fecha, c.nombre, c.celular, p.especialidad, p.modalidad, 
                p.cuota, p.tipo_de_cuota, p.banco, p.destino, p.numero_operacion, 
                c.dni, c.correo, c.genero, p.asesor, c.id, c.estado, 
                p.monto_total_diplomado, p.numero_cuota, p.proxima_fecha_pago,
                CASE 
                    WHEN p.numero_cuota = 0 THEN 'FINALIZADO'
                    WHEN p.proxima_fecha_pago < CURDATE() THEN 'DEUDA'
                    WHEN p.proxima_fecha_pago = CURDATE() THEN 'POR VENCER'
                    ELSE 'AL DIA'
                END AS estado_dinamico
            FROM clientes c
            LEFT JOIN pagos p ON c.id = p.cliente_id
            WHERE (c.nombre LIKE %s OR c.dni LIKE %s)
        """
        
        # Parámetros iniciales para el LIKE
        params = [f"%{query}%", f"%{query}%"]

        # 2. Filtros adicionales dinámicos
        if solo_inactivos:
            sql += " AND c.estado = 'inactivo'"
        else:
            sql += " AND c.estado = 'activo'"

        if especialidad:
            sql += " AND p.especialidad = %s"
            params.append(especialidad)

        if fecha_vence:
            sql += " AND p.proxima_fecha_pago = %s"
            params.append(fecha_vence)

        # 3. EL ORDEN ES VITAL: Debe ir después de todos los WHERE
        # Añadimos un espacio al inicio para evitar que se pegue al texto anterior
        sql += " ORDER BY p.id DESC"

        # 4. Ejecución
        cursor.execute(sql, params)
        resultados = cursor.fetchall()
        
        return resultados

    except Exception as e:
        print(f"ERROR en buscar_pagos_completos: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()
        
# Reutilizamos la misma lógica para morosos y hoy para evitar descuadres
def obtener_vencimientos_hoy():
    return buscar_pagos_completos("hoy") # O la lógica de fecha si prefieres, pero con 21 columnas

def obtener_clientes_en_deuda():
    return buscar_pagos_completos("deuda")


def obtener_pago_por_id(pago_id):
    """
    Obtiene un registro de pago y sus datos de cliente asociados por el ID del pago.
    """
    conn = None
    cursor = None
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
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def actualizar_pago(pago_id, form_data):
    """
    Actualiza los campos de un registro de pago existente, incluyendo
    los nuevos campos de control de deuda y analítica.
    """
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        # 1. Definimos todos los campos, incluyendo los nuevos de cobranza
        campos_pago = [
            "fecha",
            "cuota",
            "tipo_de_cuota",
            "banco",
            "destino",
            "numero_operacion",
            "especialidad",
            "modalidad",
            "asesor",
            "monto_total_diplomado",  # Nuevo
            "numero_cuota",  # Nuevo
            "proxima_fecha_pago",  # Nuevo
        ]

        # 2. Preparamos los datos (Manejo de nombres de formulario y valores vacíos)
        data = form_data.copy()

        # Mapeo de num_operacion si viene del formulario antiguo
        if "num_operacion" in data:
            data["numero_operacion"] = data.pop("num_operacion")

        # Asegurar que la fecha de renovación sea None si está vacía para la BD
        if not data.get("proxima_fecha_pago"):
            data["proxima_fecha_pago"] = None

        # 3. Construcción dinámica de la consulta SQL
        set_clause = ", ".join([f"{campo} = %s" for campo in campos_pago])
        sql = f"UPDATE pagos SET {set_clause} WHERE id = %s"

        valores = [data.get(campo) for campo in campos_pago] + [pago_id]
        cursor.execute(sql, tuple(valores))

        # 4. LÓGICA DE ESTADO: Actualizar automáticamente al cliente según la cuota editada
        # Obtenemos el cliente_id asociado a este pago para actualizar su estado general
        cursor.execute("SELECT cliente_id FROM pagos WHERE id = %s", (pago_id,))
        resultado = cursor.fetchone()

        if resultado:
            cliente_id = resultado[0]
            # Si el usuario editó la cuota a "0" (Pago Completo), el cliente queda FINALIZADO
            n_cuota = data.get("numero_cuota")
            nuevo_estado = (
                "FINALIZADO"
                if n_cuota is not None and str(n_cuota) == "0"
                else "AL DIA"
            )

            cursor.execute(
                "UPDATE clientes SET estado_pago = %s WHERE id = %s",
                (nuevo_estado, cliente_id),
            )

        conn.commit()
        return cursor.rowcount

    except Error as e:
        print(f"ERROR EN BD (actualizar_pago): {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def eliminar_pago(pago_id):
    """
    Elimina un registro de pago de la base de datos.
    (Función de Repo 1)
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


def generar_reporte_asesores_db(start_date_str=None, end_date_str=None):
    """
    Genera un reporte de ventas agrupado por asesor con filtro de fecha.
    """
    conn = None
    cursor = None
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
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def generar_excel_dinamico(headers):
    """
    Crea un archivo Excel en memoria con todos los registros de pagos.
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
        df["FECHA"] = pd.to_datetime(df["FECHA"], errors="coerce").dt.strftime(
            "%Y-%m-%d"
        )
        df.fillna("", inplace=True)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Registros")
        output.seek(0)
        return output
    except Error as e:
        print(f"ERROR EN BD (generar_excel_dinamico): {e}")
        return None
    finally:
        if conn and conn.is_connected():
            conn.close()


# --- Módulo de Auditoría ---


def registrar_auditoria(usuario, accion, ip, tabla=None, reg_id=None, detalles=None):
    """
    Inserta un nuevo registro en la tabla de auditoría.
    """
    conn = None
    cursor = None
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
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def leer_log_auditoria():
    """
    Lee todos los registros de la tabla de auditoría ordenados por fecha.
    """
    conn = None
    cursor = None
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
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


# ====================================================================
# --- ✅ INICIO: NUEVAS FUNCIONES DEL CRM (DE REPO 2) ---
# ====================================================================

# --- Funciones de CRM (Clientes y Leads) ---


def cambiar_estado_cliente(cliente_id, nuevo_estado):
    """
    Cambia el estado de un cliente a 'activo' o 'inactivo'.
    """
    conn = None
    cursor = None
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
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def obtener_cliente_por_id(cliente_id):
    """Obtiene los datos de un único cliente por su ID."""
    conn = None
    cursor = None
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
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def obtener_pagos_por_cliente(cliente_id):
    """Obtiene todos los pagos de un cliente para construir su historial de cuotas."""
    conn = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        # Ordenamos por fecha descendente para ver el pago más reciente arriba
        sql = "SELECT * FROM pagos WHERE cliente_id = %s ORDER BY fecha DESC"
        cursor.execute(sql, (cliente_id,))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (obtener_pagos_por_cliente): {e}")
        return []
    finally:
        if conn and conn.is_connected():
            conn.close()


def registrar_potencial(data, asesor):
    """
    Crea un nuevo cliente con estado 'potencial' (un lead).
    Evita duplicados por DNI o correo si ya existen.
    """
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        # 1. Verificar si ya existe (solo si se proporciona DNI o correo)
        dni = data.get("dni")
        correo = data.get("correo")

        if dni or correo:
            sql_buscar = "SELECT id FROM clientes WHERE "
            params = []
            if dni:
                sql_buscar += "dni = %s"
                params.append(dni)
            if correo:
                if dni:
                    sql_buscar += " OR "
                sql_buscar += "correo = %s"
                params.append(correo)

            cursor.execute(sql_buscar, tuple(params))
            if cursor.fetchone():
                return None  # Devuelve None si ya existe

        # 2. Si no existe, lo creamos como potencial
        sql_crear = """
            INSERT INTO clientes 
            (nombre, dni, correo, celular, genero, estado, curso_interes, asesor_asignado, fecha_contacto)
            VALUES (%s, %s, %s, %s, %s, 'potencial', %s, %s, %s)
        """
        cliente_tuple = (
            data.get("cliente"),
            data.get("dni"),
            data.get("correo"),
            data.get("celular"),
            data.get("genero"),
            data.get("curso_interes"),
            asesor,
            datetime.now(),
        )
        cursor.execute(sql_crear, cliente_tuple)
        conn.commit()
        return cursor.lastrowid

    except Error as e:
        print(f"ERROR EN BD (registrar_potencial): {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def buscar_leads(query):
    """Busca clientes que son potenciales o inactivos."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """
            SELECT id, nombre, celular, dni, correo, genero, estado, curso_interes, asesor_asignado, fecha_contacto
            FROM clientes
            WHERE (estado = 'potencial' OR estado = 'inactivo') 
              AND (nombre LIKE %s OR dni LIKE %s OR correo LIKE %s)
            ORDER BY fecha_contacto DESC
        """
        search_term = f"%{query}%"
        cursor.execute(sql, (search_term, search_term, search_term))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (buscar_leads): {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def generar_excel_leads():
    """Crea un archivo Excel en memoria con todos los leads."""
    conn = None
    try:
        conn = get_connection()
        sql = """
            SELECT nombre, celular, dni, correo, genero, estado, curso_interes, asesor_asignado, fecha_contacto
            FROM clientes
            WHERE estado = 'potencial' OR estado = 'inactivo'
            ORDER BY fecha_contacto DESC
        """
        df = pd.read_sql(sql, conn)
        df.columns = [
            "Nombre",
            "Celular",
            "DNI",
            "Correo",
            "Género",
            "Estado",
            "Curso de Interés",
            "Asesor Asignado",
            "Fecha de Contacto",
        ]

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Leads")
        output.seek(0)
        return output
    except Error as e:
        print(f"ERROR EN BD (generar_excel_leads): {e}")
        return None
    finally:
        if conn and conn.is_connected():
            conn.close()


def eliminar_lead_por_id(cliente_id):
    """Elimina un cliente y todos sus datos asociados de forma permanente."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = "DELETE FROM clientes WHERE id = %s"
        cursor.execute(sql, (cliente_id,))
        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (eliminar_lead_por_id): {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


# --- Funciones para la Gestión de Oportunidades (Embudo) ---


def crear_oportunidad_si_no_existe(cliente_id, asesor, curso):
    """Crea una oportunidad para un nuevo lead si no tiene una ya."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = """
            INSERT IGNORE INTO oportunidades 
            (cliente_id, asesor_asignado, curso_interes, estado_oportunidad, fecha_creacion, ultima_actualizacion)
            VALUES (%s, %s, %s, 'Nuevo', %s, %s)
        """
        now = datetime.now()
        cursor.execute(sql, (cliente_id, asesor, curso, now, now))
        conn.commit()
    except Error as e:
        print(f"ERROR EN BD (crear_oportunidad_si_no_existe): {e}")
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def obtener_oportunidades_por_asesor(asesor_nombre):
    """Obtiene todas las oportunidades de un asesor, uniendo datos del cliente."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """
            SELECT o.*, c.nombre as cliente_nombre, c.celular as cliente_celular
            FROM oportunidades o
            JOIN clientes c ON o.cliente_id = c.id
            WHERE o.asesor_asignado = %s AND o.estado_oportunidad NOT IN ('Ganada', 'Perdida')
            ORDER BY o.ultima_actualizacion DESC
        """
        cursor.execute(sql, (asesor_nombre,))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (obtener_oportunidades_por_asesor): {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def mover_oportunidad(oportunidad_id, nuevo_estado):
    """Actualiza el estado y la fecha de cierre si es 'Ganada' o 'Perdida'."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        now = datetime.now()

        if nuevo_estado in ("Ganada", "Perdida"):
            sql = "UPDATE oportunidades SET estado_oportunidad = %s, ultima_actualizacion = %s, fecha_cierre = %s WHERE id = %s"
            cursor.execute(sql, (nuevo_estado, now, now, oportunidad_id))
        else:
            sql = "UPDATE oportunidades SET estado_oportunidad = %s, ultima_actualizacion = %s WHERE id = %s"
            cursor.execute(sql, (nuevo_estado, now, oportunidad_id))

        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (mover_oportunidad): {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


# --- Funciones para la Gestión de Etiquetas ---
def obtener_o_crear_etiqueta_id(nombre_etiqueta):
    """Busca una etiqueta por nombre. Si no existe, la crea. Devuelve el ID."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT id FROM etiquetas WHERE LOWER(nombre) = LOWER(%s)",
            (nombre_etiqueta,),
        )
        etiqueta = cursor.fetchone()
        if etiqueta:
            return etiqueta["id"]
        else:
            cursor.execute(
                "INSERT INTO etiquetas (nombre) VALUES (%s)", (nombre_etiqueta,)
            )
            conn.commit()
            return cursor.lastrowid
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def anadir_etiqueta_a_cliente(cliente_id, etiqueta_id):
    """Vincula una etiqueta a un cliente en la tabla intermedia."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT IGNORE INTO cliente_etiquetas (cliente_id, etiqueta_id) VALUES (%s, %s)",
            (cliente_id, etiqueta_id),
        )
        conn.commit()
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def quitar_etiqueta_a_cliente(cliente_id, etiqueta_id):
    """Elimina el vínculo entre un cliente y una etiqueta."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM cliente_etiquetas WHERE cliente_id = %s AND etiqueta_id = %s",
            (cliente_id, etiqueta_id),
        )
        conn.commit()
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def obtener_etiquetas_por_cliente(cliente_id):
    """Obtiene todas las etiquetas de un cliente específico."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """
            SELECT e.id, e.nombre 
            FROM etiquetas e
            JOIN cliente_etiquetas ce ON e.id = ce.etiqueta_id
            WHERE ce.cliente_id = %s
            ORDER BY e.nombre
        """
        cursor.execute(sql, (cliente_id,))
        return cursor.fetchall()
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


# --- Funciones de Seguimiento (Reemplazan Tareas/Interacciones) ---


def crear_seguimiento(cliente_id, asesor_nombre, tipo_interaccion, comentarios):
    """
    Crea un nuevo registro de seguimiento con estado 'Por Atender'.
    """
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        zona_peru = pytz.timezone("America/Lima")
        fecha_actual = datetime.now(zona_peru)

        sql = """
            INSERT INTO seguimientos
            (cliente_id, asesor_nombre, fecha_creacion, tipo_interaccion, comentarios, estado)
            VALUES (%s, %s, %s, %s, %s, 'Por Atender')
        """
        datos = (cliente_id, asesor_nombre, fecha_actual, tipo_interaccion, comentarios)
        cursor.execute(sql, datos)
        conn.commit()
        return cursor.lastrowid
    except Error as e:
        print(f"ERROR EN BD (crear_seguimiento): {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def obtener_seguimientos_por_cliente(cliente_id):
    """
    Obtiene todos los seguimientos de un cliente, ordenados por fecha.
    """
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = "SELECT * FROM seguimientos WHERE cliente_id = %s ORDER BY fecha_creacion DESC"
        cursor.execute(sql, (cliente_id,))
        return cursor.fetchall()
    except Error as e:
        print(f"ERROR EN BD (obtener_seguimientos_por_cliente): {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def marcar_seguimiento_atendido(seguimiento_id):
    """
    Actualiza el estado de un seguimiento a 'Atendido' y registra la fecha.
    """
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()

        zona_peru = pytz.timezone("America/lima")
        fecha_actual = datetime.now(zona_peru)

        sql = "UPDATE seguimientos SET estado = 'Atendido', fecha_atencion = %s WHERE id = %s"
        cursor.execute(sql, (fecha_actual, seguimiento_id))
        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (marcar_seguimiento_atendido): {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def eliminar_seguimiento(seguimiento_id):
    """
    Elimina un registro de seguimiento de forma permanente.
    """
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = "DELETE FROM seguimientos WHERE id = %s"
        cursor.execute(sql, (seguimiento_id,))
        conn.commit()
        return cursor.rowcount
    except Error as e:
        print(f"ERROR EN BD (eliminar_seguimiento): {e}")
        raise e
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


# --- Funciones de Indicadores (KPI) ---


def calcular_tiempo_gestion_promedio():
    """Calcula el tiempo promedio (en días) desde creación hasta cierre (Ganada/Perdida)."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """
            SELECT AVG(DATEDIFF(fecha_cierre, fecha_creacion)) as tiempo_promedio_dias
            FROM oportunidades
            WHERE estado_oportunidad IN ('Ganada', 'Perdida') AND fecha_cierre IS NOT NULL
        """
        cursor.execute(sql)
        resultado = cursor.fetchone()
        return (
            resultado["tiempo_promedio_dias"]
            if resultado and resultado["tiempo_promedio_dias"] is not None
            else 0
        )
    except Error as e:
        print(f"ERROR EN BD (calcular_tiempo_gestion_promedio): {e}")
        return 0
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def calcular_cumplimiento_primer_contacto(horas_limite=24):
    """Calcula el % de leads contactados (primer seguimiento) dentro de las horas_limite."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        sql = """
            SELECT
                c.id,
                c.fecha_contacto as fecha_registro,
                MIN(s.fecha_creacion) as fecha_primer_seguimiento
            FROM clientes c
            LEFT JOIN seguimientos s ON c.id = s.cliente_id
            WHERE c.estado IN ('potencial', 'activo')
            GROUP BY c.id, c.fecha_contacto
        """
        cursor.execute(sql)
        leads = cursor.fetchall()
        if not leads:
            return 0.0

        cumplen = 0
        total_evaluados = 0
        for lead in leads:
            if lead["fecha_registro"]:
                total_evaluados += 1
                if (
                    lead["fecha_primer_seguimiento"]
                    and (
                        lead["fecha_primer_seguimiento"] - lead["fecha_registro"]
                    ).total_seconds()
                    <= horas_limite * 3600
                ):
                    cumplen += 1
        return (cumplen / total_evaluados) * 100 if total_evaluados > 0 else 0.0
    except Error as e:
        print(f"ERROR EN BD (calcular_cumplimiento_primer_contacto): {e}")
        return 0.0
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def calcular_porcentaje_leads_con_seguimiento():
    """Calcula el % de clientes (potenciales o activos) que tienen al menos un seguimiento registrado."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        sql_seguimientos = (
            "SELECT COUNT(DISTINCT cliente_id) as con_seguimiento FROM seguimientos"
        )
        cursor.execute(sql_seguimientos)
        con_seguimiento = (cursor.fetchone() or {}).get("con_seguimiento", 0)

        sql_total = "SELECT COUNT(*) as total FROM clientes WHERE estado IN ('potencial', 'activo')"
        cursor.execute(sql_total)
        total_clientes = (cursor.fetchone() or {}).get("total", 0)

        return (con_seguimiento / total_clientes) * 100 if total_clientes > 0 else 0.0
    except Error as e:
        print(f"ERROR EN BD (calcular_porcentaje_leads_con_seguimiento): {e}")
        return 0.0
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def calcular_porcentaje_seguimientos_atendidos(asesor_nombre=None):
    """Calcula el % de seguimientos 'Atendidos' (opcionalmente filtrado por asesor)."""
    conn = None
    cursor = None
    try:
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)
        base_sql = "SELECT COUNT(*) as count FROM seguimientos"
        params = []
        where_clause = ""
        if asesor_nombre:
            where_clause = " WHERE asesor_nombre = %s"
            params.append(asesor_nombre)

        sql_atendidos = (
            base_sql
            + where_clause
            + (" AND " if where_clause else " WHERE ")
            + "estado = 'Atendido'"
        )
        cursor.execute(sql_atendidos, tuple(params))
        atendidos = (cursor.fetchone() or {}).get("count", 0)

        sql_total = base_sql + where_clause
        cursor.execute(sql_total, tuple(params))
        total = (cursor.fetchone() or {}).get("count", 0)

        return (atendidos / total) * 100 if total > 0 else 0.0
    except Error as e:
        print(f"ERROR EN BD (calcular_porcentaje_seguimientos_atendidos): {e}")
        return 0.0
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


# --- Integración con Google Sheets (Funciones Antiguas) ---
# (Se mantienen por compatibilidad, pero `sheets_manager.py` es el principal)
def conectar_a_gsheets():
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive",
        ]
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        creds_file = os.path.join(
            base_dir, "credentials.json"
        )  # Asume que credentials.json está en la raíz
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
        client = gspread.authorize(creds)
        return client
    except Exception as e:
        print(f"ERROR al conectar con Google API: {e}")
        raise e


def obtener_datos_sheet(nombre_sheet):
    try:
        client = conectar_a_gsheets()
        sheet = client.open(nombre_sheet).sheet1
        registros = sheet.get_all_records()
        registros_limpios = [
            row for row in registros if any(str(val).strip() for val in row.values())
        ]
        return registros_limpios
    except Exception as e:
        print(f"ERROR al leer Google Sheet '{nombre_sheet}': {e}")
        raise e


def obtener_vencimientos_hoy():
    """Headers: 21 columnas para consulta.html"""
    conn = get_connection()
    cursor = conn.cursor()
    # Enviamos 21 valores para que los índices [16], [18], [20] del HTML funcionen
    sql = """
        SELECT p.id, p.fecha, c.nombre, c.celular, p.especialidad, p.modalidad, 
               p.cuota, p.tipo_de_cuota, p.banco, p.destino, p.numero_operacion, 
               c.dni, c.correo, c.genero, p.asesor, c.id, c.estado, 
               0, p.numero_cuota, p.proxima_fecha_pago, 'AL DIA'
        FROM pagos p
        JOIN clientes c ON p.cliente_id = c.id
        WHERE p.proxima_fecha_pago = CURDATE()
        ORDER BY c.nombre ASC
    """
    cursor.execute(sql)
    res = cursor.fetchall()
    conn.close()
    return res


def obtener_clientes_en_deuda():
    """Headers: 21 columnas para consulta.html"""
    conn = get_connection()
    cursor = conn.cursor()
    sql = """
        SELECT p.id, p.fecha, c.nombre, c.celular, p.especialidad, p.modalidad, 
               p.cuota, p.tipo_de_cuota, p.banco, p.destino, p.numero_operacion, 
               c.dni, c.correo, c.genero, p.asesor, c.id, c.estado, 
               0, p.numero_cuota, p.proxima_fecha_pago, 'DEUDA'
        FROM pagos p
        JOIN clientes c ON p.cliente_id = c.id
        WHERE p.proxima_fecha_pago < CURDATE() AND p.numero_cuota != 0
        ORDER BY p.proxima_fecha_pago ASC
    """
    cursor.execute(sql)
    res = cursor.fetchall()
    conn.close()
    return res


def consultar_pagos(query="", page=1, per_page=8):
    conn = get_connection()
    cursor = conn.cursor()
    offset = (page - 1) * per_page
    query = query.lower().strip() if query else ""

    # 1. Lógica para filtros rápidos (Morosos / Vencen Hoy)
    if query == "deuda":
        sql = """SELECT p.id, p.fecha, c.nombre, c.celular, p.especialidad, p.modalidad, p.cuota, 
                        p.tipo_de_cuota, p.banco, p.destino, p.numero_operacion, c.dni, c.correo, 
                        c.genero, p.asesor, c.id, c.estado, 0, p.numero_cuota, p.proxima_fecha_pago, 'DEUDA' 
                 FROM pagos p JOIN clientes c ON p.cliente_id = c.id 
                 WHERE p.proxima_fecha_pago < CURDATE() AND p.numero_cuota != 0 
                 ORDER BY p.fecha DESC LIMIT %s OFFSET %s"""
        count_sql = "SELECT COUNT(*) FROM pagos WHERE proxima_fecha_pago < CURDATE() AND numero_cuota != 0"
        params = (per_page, offset)
        count_params = ()
    elif query == "hoy":
        sql = """SELECT p.id, p.fecha, c.nombre, c.celular, p.especialidad, p.modalidad, p.cuota, 
                        p.tipo_de_cuota, p.banco, p.destino, p.numero_operacion, c.dni, c.correo, 
                        c.genero, p.asesor, c.id, c.estado, 0, p.numero_cuota, p.proxima_fecha_pago, 'AL DIA' 
                 FROM pagos p JOIN clientes c ON p.cliente_id = c.id 
                 WHERE p.proxima_fecha_pago = CURDATE() 
                 ORDER BY p.fecha DESC LIMIT %s OFFSET %s"""
        count_sql = "SELECT COUNT(*) FROM pagos WHERE proxima_fecha_pago = CURDATE()"
        params = (per_page, offset)
        count_params = ()
    else:
        # 2. Búsqueda general (Si query está vacío, muestra TODOS por el LIKE %%)
        sql = """SELECT p.id, p.fecha, c.nombre, c.celular, p.especialidad, p.modalidad, p.cuota, 
                        p.tipo_de_cuota, p.banco, p.destino, p.numero_operacion, c.dni, c.correo, 
                        c.genero, p.asesor, c.id, c.estado, 0, p.numero_cuota, p.proxima_fecha_pago, 
                        CASE 
                            WHEN p.numero_cuota = 0 THEN 'FINALIZADO' 
                            WHEN p.proxima_fecha_pago < CURDATE() THEN 'DEUDA' 
                            ELSE 'AL DIA' 
                        END 
                 FROM pagos p JOIN clientes c ON p.cliente_id = c.id 
                 WHERE c.nombre LIKE %s OR c.dni LIKE %s 
                 ORDER BY p.id DESC LIMIT %s OFFSET %s"""
        count_sql = "SELECT COUNT(*) FROM pagos p JOIN clientes c ON p.cliente_id = c.id WHERE c.nombre LIKE %s OR c.dni LIKE %s"
        search = f"%{query}%"
        params = (search, search, per_page, offset)
        count_params = (search, search)

    cursor.execute(sql, params)
    res = cursor.fetchall()
    cursor.execute(count_sql, count_params)
    total = cursor.fetchone()[0]
    conn.close()
    return res, total
