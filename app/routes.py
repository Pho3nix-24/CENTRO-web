"""
Módulo de Rutas de Flask (Fusión Completa: Original + CRM + Analítica)
---------------------------------------------------------
Este archivo define todas las URLs de la aplicación, integrando la gestión de
pagos y certificados original con el nuevo sistema CRM y el Dashboard Analítico.
"""

# --- Importaciones ---
import os
import pytz
from datetime import datetime
from functools import wraps
from flask import (
    render_template, request, redirect, url_for,
    flash, session, send_file, send_from_directory, jsonify
)
from app import app
from app import database_manager as db
from mysql.connector import IntegrityError, Error as DB_Error
from werkzeug.security import check_password_hash
from app import sheets_manager


# --- Configuración y Constantes ---
RECORDS_PER_PAGE = 5
RECORDS_PER_PAGE_SHEETS = 20

# --- Configuración de Seguridad de Login ---
failed_logins = {}
LOGIN_ATTEMPT_LIMIT = 5
LOCKOUT_TIME_SECONDS = 300

# Diccionario de usuarios con roles definidos
USERS = {
    'admin':      {'password_hash': 'scrypt:32768:8:1$WFi0YBN2qCDwBgWJ$be6ca3584230b85b3b4fdbba30a11bf25d0b30fd525ac6c1a83708edf7401d9300ab2e3388eee7835ce47534dbfcdb4458b538db90ec960ef3852f793c869b47', 'full_name': 'Administrador',   'role': 'admin'},
    'lud_rojas':  {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72',       'full_name': 'Lud Rojas',       'role': 'equipo'},
    'ruth_lecca': {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72',       'full_name': 'Ruth Lecca',      'role': 'equipo'},
    'rafa_diaz':  {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72', 'full_name': 'Rafael Díaz',     'role': 'atencion_cliente'},
    # --- USUARIOS CRM AGREGADOS ---
    'lesly_gamboa':  {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72', 'full_name': 'Lesly Gamboa', 'role': 'crm'},
    'teresa_diaz':   {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72', 'full_name': 'Teresa Diaz',  'role': 'crm'},
    'hillary_vega':  {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72', 'full_name': 'Hillary Vega', 'role': 'crm'},
    'mariainy_guerra': {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72', 'full_name': 'Mariainy Guerra', 'role': 'crm'}
}

HEADERS = [
    "FECHA", "CLIENTE", "CELULAR", "ESPECIALIDAD", "MODALIDAD", "CUOTA",
    "TIPO DE CUOTA", "BANCO", "DESTINO", "N° OPERACIÓN", "DNI", "CORREO",
    "GÉNERO", "ASESOR"
]
FIELDS = [
    "fecha", "cliente", "celular", "especialidad", "modalidad", "cuota",
    "tipo_de_cuota", "banco", "destino", "numero_operacion", "dni",
    "correo", "genero", "asesor"
]


# --- Funciones Auxiliares ---
def get_user_ip():
    """Obtiene la IP real del usuario."""
    if request.headers.getlist("X-Forwarded-For"):
        return request.headers.getlist("X-Forwarded-For")[0]
    return request.remote_addr

def login_required(f):
    """Decorador para proteger rutas."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            flash('Debes iniciar sesión para ver esta página.', 'error')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# --- Rutas de Sesión (Login/Logout) ---

@app.route("/login", methods=["GET", "POST"])
def login():
    ip_usuario = get_user_ip()
    if ip_usuario in failed_logins:
        user_failures = failed_logins[ip_usuario]
        elapsed_time = (datetime.now() - user_failures['last_attempt_time']).total_seconds()
        if user_failures['attempts'] >= LOGIN_ATTEMPT_LIMIT and elapsed_time < LOCKOUT_TIME_SECONDS:
            tiempo_restante = round((LOCKOUT_TIME_SECONDS - elapsed_time) / 60)
            flash(f"Demasiados intentos fallidos. Por favor, espera {tiempo_restante} minutos.", "error")
            return render_template("login.html")
        if elapsed_time >= LOCKOUT_TIME_SECONDS:
            failed_logins.pop(ip_usuario, None)

    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")
        user_data = USERS.get(username)
        
        if user_data and check_password_hash(user_data['password_hash'], password):
            failed_logins.pop(ip_usuario, None)
            session['logged_in'] = True
            session['full_name'] = user_data['full_name']
            session['username'] = username
            session['role'] = user_data['role']
            db.registrar_auditoria(user_data['full_name'], "INICIO_SESION_EXITOSO", ip_usuario)
            flash("Has iniciado sesión correctamente.", "success")
            
            if session['role'] == 'crm':
                return redirect(url_for("crm_dashboard"))
            
            return redirect(url_for("menu"))
        else:
            if ip_usuario not in failed_logins:
                failed_logins[ip_usuario] = {'attempts': 0, 'last_attempt_time': datetime.now()}
            failed_logins[ip_usuario]['attempts'] += 1
            failed_logins[ip_usuario]['last_attempt_time'] = datetime.now()
            intentos_restantes = LOGIN_ATTEMPT_LIMIT - failed_logins[ip_usuario]['attempts']
            db.registrar_auditoria(username, "INICIO_SESION_FALLIDO", ip_usuario)
            
            if intentos_restantes > 0:
                flash(f"Credenciales incorrectas. Te quedan {intentos_restantes} intentos.", "error")
            else:
                flash("Demasiados intentos fallidos. Tu acceso ha sido bloqueado por 5 minutos.", "error")
            
    return render_template("login.html")

@app.route("/logout")
@login_required
def logout():
    usuario_actual = session.get('full_name', 'desconocido')
    ip_usuario = get_user_ip()
    db.registrar_auditoria(usuario_actual, "CIERRE_SESION", ip_usuario)
    session.clear()
    flash("Has cerrado la sesión.", "success")
    return redirect(url_for('login'))

@app.route("/menu")
@login_required
def menu():
    if session.get('role') == 'crm':
        return redirect(url_for('crm_dashboard'))
    return render_template("menu.html")

@app.route("/")
def index():
    if session.get('role') == 'crm':
        return redirect(url_for('crm_dashboard'))
    return redirect(url_for('dashboard'))

# ================= SECCIÓN VENTAS (Mejorada) =================

@app.route("/dashboard")
@login_required
def dashboard():
    if session.get('role') == 'crm':
        return redirect(url_for('crm_dashboard'))

    if session.get('role') == 'atencion_cliente':
        return redirect(url_for('consulta'))

    # Configuración de zona horaria para Trujillo, Perú[cite: 1]
    tz_peru = pytz.timezone('America/Lima')
    fecha_actual_peru = datetime.now(tz_peru)

    try:
        # Obtenemos las estadísticas mejoradas (incluye datos para gráficos)[cite: 1]
        estadisticas = db.obtener_estadisticas_dashboard()
        # Traemos los últimos 5 usando la nueva consulta con todas las columnas[cite: 1]
        ultimos_pagos = db.buscar_pagos_completos("")[:5]
    except Exception as e:
        flash(f"Error al cargar los datos del dashboard: {e}", "error")
        estadisticas = {
            'ingresos_hoy': 0, 'ingresos_semana': 0, 'ingresos_mes': 0, 'estimado_mensual': 0,
            'grafico_especialidades': {'labels': [], 'data': []},
            'grafico_semanal': {'labels': [], 'data': []}
        }
        ultimos_pagos = []

    return render_template(
        "dashboard.html", 
        stats=estadisticas, 
        pagos=ultimos_pagos,
        now=fecha_actual_peru,
        current_section='ventas'
    )

@app.route("/registrar", methods=["GET"])
@login_required
def registrar():
    # 1. Mantenemos tu seguridad por rol
    if session.get("role") == "atencion_cliente":
        flash("Acceso no autorizado.", "error")
        return redirect(url_for("consulta"))
    
    # 2. Capturamos el DNI que viene de la nueva interfaz de verificación
    # Si viene de "Siguiente", tendrá el DNI. Si entran directo, estará vacío.
    dni_verificado = request.args.get('dni', '')
    
    # 3. Enviamos todo al template
    return render_template(
        "index.html", 
        current_section="ventas", 
        dni_predeterminado=dni_verificado
    )


@app.route("/submit", methods=["POST"])
@login_required
def submit():
    if session.get("role") == "atencion_cliente":
        flash("Acceso no autorizado.", "error")
        return redirect(url_for("consulta"))

    form_data = request.form.to_dict()
    dni = form_data.get("dni")
    num_op = form_data.get("num_operacion") # Capturamos el voucher

    try:
        # 1. Validar Fecha
        fecha_str = form_data.get("fecha")
        if not fecha_str:
            flash("La fecha es un campo obligatorio.", "error")
            return redirect(url_for("registrar"))
        try:
            form_data["fecha"] = datetime.strptime(fecha_str, "%Y-%m-%d")
        except ValueError:
            flash("El formato de la fecha es incorrecto. Por favor, usa AAAA-MM-DD.", "error")
            return redirect(url_for("registrar"))

        # 2. Conexión para validaciones previas
        conn = db.get_connection()
        cursor = conn.cursor(dictionary=True)

        # A. VALIDAR DNI DUPLICADO
        cursor.execute("SELECT nombre FROM clientes WHERE dni = %s", (dni,))
        cliente_dni = cursor.fetchone()
        if cliente_dni:
            conn.close()
            flash(f"⛔ Error: El DNI {dni} ya pertenece al cliente {cliente_dni['nombre']}. No se permiten duplicados.", "error")
            return redirect(url_for("registrar"))

        # B. VALIDAR N° OPERACIÓN DUPLICADO (Voucher)
        # Buscamos en pagos y unimos con clientes para saber de quién es el voucher original
        cursor.execute("""
            SELECT c.nombre 
            FROM pagos p 
            JOIN clientes c ON p.cliente_id = c.id 
            WHERE p.numero_operacion = %s
        """, (num_op,))
        pago_existente = cursor.fetchone()
        conn.close()

        if pago_existente:
            flash(f"⛔ Error: El N° de Operación {num_op} ya fue registrado por: {pago_existente['nombre']}.", "error")
            return redirect(url_for("registrar"))

        # 3. SI TODO ESTÁ OK, PROCEDEMOS AL GUARDADO
        # Buscamos o creamos el ID (aunque aquí ya sabemos que es nuevo por el paso anterior)
        cliente_id = db.buscar_o_crear_cliente(form_data)
        form_data["numero_operacion"] = form_data.pop("num_operacion", None)

        # Guardar con los nuevos campos de cobranza en la tabla pagos
        nuevo_pago_id = db.crear_pago(cliente_id, form_data)
        
        # 4. Auditoría
        usuario_actual = session.get("full_name", "desconocido")
        ip_usuario = get_user_ip()
        detalles = f"Cliente ID: {cliente_id}, Pago ID: {nuevo_pago_id}"
        db.registrar_auditoria(
            usuario_actual, "CREAR_PAGO", ip_usuario, "pagos", nuevo_pago_id, detalles
        )

        flash("✅ Registro guardado correctamente.", "success")

    except IntegrityError:
        flash("Error de integridad: El DNI, Correo o Voucher ya existe en el sistema.", "error")
    except DB_Error as e:
        flash(f"Error de base de datos: {e}", "error")

    return redirect(url_for("registrar"))

# ================= SECCIÓN CONSULTA Y GESTIÓN DE PAGOS =================
@app.route("/consulta", methods=["GET"])
@login_required
def consulta():
    if session.get("role") == "crm":
        return redirect(url_for("crm_dashboard"))

    # 1. CAPTURAR PARÁMETROS DE FILTRO
    query = request.args.get("query", "").strip().lower()
    fecha_vence = request.args.get("fecha_vence")  # Filtro de calendario
    especialidad = request.args.get("especialidad")  # Filtro de especialidad
    estado_filtro = request.args.get("estado")  # Para detectar 'inactivo'
    page = request.args.get("page", 1, type=int)

    resultados_paginados = []
    total_pages = 1

    try:
        # 2. LÓGICA DE FILTRADO INTEGRADA
        # Usamos la nueva versión de buscar_pagos_completos que acepta todos los filtros
        if query == "hoy":
            todos_los_resultados = db.obtener_vencimientos_hoy()
        elif query == "deuda":
            todos_los_resultados = db.obtener_clientes_en_deuda()
        else:
            # Esta función ahora centraliza la búsqueda por texto, fecha, especialidad y estado
            todos_los_resultados = db.buscar_pagos_completos(
                query=query, 
                fecha_vence=fecha_vence, 
                especialidad=especialidad, 
                solo_inactivos=(estado_filtro == 'inactivo')
            )

        # 3. PAGINACIÓN[cite: 1]
        total_records = len(todos_los_resultados)
        total_pages = (
            (total_records + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE
            if RECORDS_PER_PAGE > 0
            else 1
        )
        start_index = (page - 1) * RECORDS_PER_PAGE
        end_index = start_index + RECORDS_PER_PAGE
        resultados_paginados = todos_los_resultados[start_index:end_index]

    except DB_Error as e:
        flash(f"Error al consultar la base de datos: {e}", "error")

    # 4. RENDERIZAR PLANTILLA CON VARIABLES DE ESTADO
    return render_template(
        "consulta.html",
        resultados=resultados_paginados,
        page=page,
        total_pages=total_pages,
        query=query,
        fecha_actual=fecha_vence,      # Para mantener la fecha en el input date
        especialidad_actual=especialidad, # Para mantener el select seleccionado
        estado_actual=estado_filtro,    # Para saber si estamos viendo inactivos
        current_section="ventas"
    )

    
@app.route("/editar/<int:id>", methods=["GET", "POST"])
@login_required
def editar(id):
    if request.method == "POST":
        form_data = request.form.to_dict()
        try:
            db.actualizar_pago(id, form_data)
            flash("Pago actualizado correctamente.", "success")
            usuario_actual = session.get('full_name', 'desconocido')
            ip_usuario = get_user_ip()
            db.registrar_auditoria(usuario_actual, "EDITAR_PAGO", ip_usuario, "pagos", id)
        except DB_Error as e:
            flash(f"Error al actualizar el pago: {e}", "error")
        return redirect(url_for("consulta", query=form_data.get("query", "")))
    
    data = db.obtener_pago_por_id(id)
    headers_edicion = ["FECHA", "CLIENTE", "CELULAR", "ESPECIALIDAD", "MODALIDAD", "CUOTA", "TIPO DE CUOTA", "BANCO", "DESTINO", "N° OPERACIÓN", "DNI", "CORREO", "GÉNERO", "ASESOR"]
    labels_and_fields = list(zip(headers_edicion, FIELDS))
    return render_template("editar.html", data=data, labels_and_fields=labels_and_fields, id=id, query=request.args.get('query', ''), current_section='ventas')

@app.route("/actualizar_pago/<int:id>", methods=["GET", "POST"])
@login_required
def actualizar_pago(id):
    if session.get('role') == 'atencion_cliente':
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))

    if request.method == "POST":
        query = request.form.get("query", "")
        try:
            pago_original = db.obtener_pago_por_id(id)
            if not pago_original:
                flash("Error: No se encontró el registro original.", "error")
                return redirect(url_for("consulta", query=query))

            cliente_id = pago_original['cliente_id']
            datos_nuevo_pago = request.form.to_dict()
            
            # Mantener lógica original y persistencia[cite: 1]
            datos_nuevo_pago['fecha'] = datetime.now()
            datos_nuevo_pago['numero_operacion'] = datos_nuevo_pago.pop('num_operacion', None)
            datos_nuevo_pago['especialidad'] = pago_original['especialidad']
            datos_nuevo_pago['modalidad'] = pago_original['modalidad']
            datos_nuevo_pago['asesor'] = pago_original['asesor']
            datos_nuevo_pago['monto_total_diplomado'] = pago_original['monto_total_diplomado']
            
            nuevo_pago_id = db.crear_pago(cliente_id, datos_nuevo_pago)
            flash("Renovación de pago registrada exitosamente.", "success")
            
            usuario_actual = session.get('full_name', 'desconocido')
            ip_usuario = get_user_ip()
            detalles = f"Cliente ID: {cliente_id}, Pago ID: {nuevo_pago_id} (RENOVACIÓN)"
            db.registrar_auditoria(usuario_actual, "RENOVAR_PAGO", ip_usuario, "pagos", nuevo_pago_id, detalles)

            return redirect(url_for("consulta", query=query))

        except IntegrityError:
            flash("Error: El N° de Operación ingresado ya existe en otro registro. Por favor, verifícalo.", "error")
            return redirect(url_for("actualizar_pago", id=id, query=query))
        except DB_Error as e:
            flash(f"Error al procesar el pago: {e}", "error")
            return redirect(url_for("consulta", query=query))
    
    query = request.args.get('query', '')
    datos_pago_actual = db.obtener_pago_por_id(id)
    return render_template("actualizar_pago.html", data=datos_pago_actual, id=id, query=query, current_section='ventas')

@app.route("/desactivar_cliente", methods=["POST"])
@login_required
def desactivar_cliente():
    if session.get('role') == 'atencion_cliente':
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))

    pago_id = int(request.form.get("id"))
    query = request.form.get("query", "")
    try:
        pago = db.obtener_pago_por_id(pago_id)
        if pago:
            cliente_id = pago['cliente_id']
            db.cambiar_estado_cliente(cliente_id, 'inactivo')
            flash("Cliente desactivado correctamente.", "success")
            usuario_actual = session.get('full_name', 'desconocido')
            ip_usuario = get_user_ip()
            db.registrar_auditoria(usuario_actual, "DESACTIVAR_CLIENTE", ip_usuario, "clientes", cliente_id)
        else:
            flash("Error: No se encontró el pago asociado.", "error")
    except DB_Error as e:
        flash(f"Error: No se pudo desactivar al cliente: {e}", "error")
    return redirect(url_for("consulta", query=query))

@app.route("/reactivar_cliente", methods=["POST"])
@login_required
def reactivar_cliente():
    if session.get('role') not in ['admin', 'equipo']:
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))

    cliente_id = int(request.form.get("cliente_id"))
    query = request.form.get("query", "")
    try:
        db.cambiar_estado_cliente(cliente_id, 'activo')
        flash("Cliente reactivado correctamente.", "success")
        usuario_actual = session.get('full_name', 'desconocido')
        ip_usuario = get_user_ip()
        db.registrar_auditoria(usuario_actual, "REACTIVAR_CLIENTE", ip_usuario, "clientes", cliente_id)
    except DB_Error as e:
        flash(f"Error al reactivar al cliente: {e}", "error")
    return redirect(url_for("consulta", query=query))

@app.route("/eliminar_pago", methods=["POST"])
@login_required
def eliminar_pago():
    if session.get('role') not in ['admin', 'equipo']:
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))

    pago_id = int(request.form.get("id"))
    query = request.form.get("query", "")
    try:
        filas_afectadas = db.eliminar_pago(pago_id)
        if filas_afectadas > 0:
            flash(f"Registro de pago ID: {pago_id} eliminado permanentemente.", "success")
            usuario_actual = session.get('full_name', 'desconocido')
            ip_usuario = get_user_ip()
            db.registrar_auditoria(usuario_actual, "ELIMINAR_PAGO_PERMANENTE", ip_usuario, "pagos", pago_id)
        else:
            flash("Error: No se encontró el registro de pago para eliminar.", "error")
    except DB_Error as e:
        flash(f"Error de base de datos al eliminar el pago: {e}", "error")
    return redirect(url_for("consulta", query=query))

@app.route("/reportes")
@login_required
def reportes():
    if session.get('role') == 'atencion_cliente':
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        page = request.args.get('page', 1, type=int)

        reporte_completo_db = db.generar_reporte_asesores_db(start_date, end_date)
        total_records = len(reporte_completo_db)
        total_pages = (total_records + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE if RECORDS_PER_PAGE > 0 else 1
        start_index = (page - 1) * RECORDS_PER_PAGE
        end_index = start_index + RECORDS_PER_PAGE
        reporte_paginado_db = reporte_completo_db[start_index:end_index]

        total_general_ventas = sum(item.get('total_asesor', 0) or 0 for item in reporte_completo_db)
        total_general_registros = sum(item.get('registros_asesor', 0) for item in reporte_completo_db)
        page_total_ventas = sum(item.get('total_asesor', 0) or 0 for item in reporte_paginado_db)
        page_total_registros = sum(item.get('registros_asesor', 0) for item in reporte_paginado_db)
        
        reporte_para_plantilla = [(item['asesor'], item) for item in reporte_paginado_db]
        
        return render_template(
            "reportes.html", reporte=reporte_para_plantilla, total_ventas=total_general_ventas,
            total_registros=total_general_registros, start_date=start_date, end_date=end_date,
            page=page, total_pages=total_pages, page_total_ventas=page_total_ventas,
            page_total_registros=page_total_registros, current_section='ventas'
        )
    except DB_Error as e:
        flash(f"Error al generar el reporte: {e}", "error")
        return render_template("reportes.html", reporte=[], current_section='ventas')

@app.route("/descargar")
@login_required
def descargar():
    if session.get('role') == 'atencion_cliente':
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))
    try:
        output = db.generar_excel_dinamico(HEADERS)
        if output:
            return send_file(
                output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True, download_name='registros_db.xlsx'
            )
        else:
            flash("Error al generar el archivo Excel.", "error")
            return redirect(url_for('index'))
    except DB_Error as e:
        flash(f"Error al generar el archivo Excel: {e}", "error")
        return redirect(url_for('index'))

@app.route("/auditoria")
@login_required
def auditoria():
    if session.get('username') != 'admin':
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('index'))
    try:
        todos_los_logs = db.leer_log_auditoria()
        return render_template("auditoria.html", logs=todos_los_logs, current_section='ventas')
    except DB_Error as e:
        flash(f"Error al leer la auditoría: {e}", "error")
        return render_template("auditoria.html", logs=[], current_section='ventas')


# ================= SECCIÓN CRM (FUNCIONALIDAD COMPLETA) =================

@app.route("/crm")
@login_required
def crm_dashboard():
    if session.get('role') not in ['admin', 'equipo', 'crm']:
        return redirect(url_for('menu'))
    return render_template("crm_dashboard.html", current_section='crm')

@app.route("/registrar_interesado", methods=['GET', 'POST'])
@login_required
def registrar_interesado():
    if session.get('role') not in ['admin', 'equipo', 'crm']:
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))
    
    if request.method == 'POST':
        form_data = request.form.to_dict()
        asesor_actual = session.get('full_name', 'desconocido')
        ip_usuario = get_user_ip()
        
        if 'cliente' in form_data and 'nombre' not in form_data:
            form_data['nombre'] = form_data['cliente']
        
        try:
            nuevo_id = db.registrar_potencial(form_data, asesor_actual)
            if nuevo_id:
                db.crear_oportunidad_si_no_existe(nuevo_id, asesor_actual, form_data.get('curso_interes'))
                flash("Cliente potencial registrado exitosamente.", "success")
                db.registrar_auditoria(asesor_actual, "CREAR_POTENCIAL", ip_usuario, "clientes", nuevo_id)
            else:
                flash("Ya existe un cliente con ese DNI o correo.", "error")
        except DB_Error as e:
            flash(f"Error al registrar al cliente potencial: {e}", "error")
        
        return redirect(url_for('registrar_interesado'))
    
    return render_template("registrar_interesado.html", current_section='crm')

@app.route("/oportunidades")
@login_required
def oportunidades():
    if session.get('role') not in ['admin', 'equipo', 'crm']:
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))

    asesor_actual = session.get('full_name')
    etapas = ['Nuevo', 'Contactado', 'Propuesta', 'Negociación']
    oportunidades_por_etapa = {etapa: [] for etapa in etapas}
    
    try:
        todas_las_oportunidades = db.obtener_oportunidades_por_asesor(asesor_actual)
        for op in todas_las_oportunidades:
            if op['estado_oportunidad'] in oportunidades_por_etapa:
                oportunidades_por_etapa[op['estado_oportunidad']].append(op)
    except DB_Error as e:
        flash(f"Error al cargar las oportunidades: {e}", "error")

    return render_template("oportunidades.html", oportunidades_por_etapa=oportunidades_por_etapa, etapas=etapas, current_section='crm')

@app.route("/api/oportunidad/mover", methods=['POST'])
@login_required
def mover_oportunidad_api():
    data = request.get_json()
    try:
        db.mover_oportunidad(data.get('id'), data.get('estado'))
        return jsonify({"status": "success"}), 200
    except DB_Error as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/crm/consulta")
@login_required
def consulta_leads():
    if session.get('role') not in ['admin', 'equipo', 'crm']:
        return redirect(url_for('dashboard'))
    
    query = request.args.get("query", "").strip()
    try:
        leads = db.buscar_leads(query)
    except DB_Error as e:
        flash(f"Error al consultar los leads: {e}", "error")
        leads = []
    return render_template("consulta_leads.html", leads=leads, query=query, current_section='crm')

@app.route("/crm/descargar_leads")
@login_required
def descargar_leads():
    if session.get('role') not in ['admin', 'equipo', 'crm']:
        return redirect(url_for('dashboard'))
    try:
        output = db.generar_excel_leads()
        if output:
            return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', as_attachment=True, download_name='registros_leads.xlsx')
    except DB_Error as e:
        flash(f"Error al generar Excel: {e}", "error")
    return redirect(url_for('crm_dashboard'))

@app.route("/crm/lead/eliminar/<int:cliente_id>", methods=["POST"])
@login_required
def eliminar_lead(cliente_id):
    try:
        db.eliminar_lead_por_id(cliente_id)
        flash("Lead eliminado permanentemente.", "success")
    except DB_Error as e:
        flash(f"Error: {e}", "error")
    return redirect(url_for('consulta_leads'))

@app.route("/crm/indicadores")
@login_required
def reporte_indicadores():
    if session.get('role') not in ['admin', 'equipo', 'crm']:
        return redirect(url_for('menu'))
        
    try:
        kpis = {
            'tiempo_promedio_gestion': db.calcular_tiempo_gestion_promedio(),
            'cumplimiento_primer_contacto': db.calcular_cumplimiento_primer_contacto(24),
            'leads_con_interaccion': db.calcular_porcentaje_leads_con_seguimiento(),
            'tareas_completadas_total': db.calcular_porcentaje_seguimientos_atendidos()
        }
    except DB_Error:
        kpis = {}
    return render_template("reporte_indicadores.html", indicadores=kpis, current_section='crm')

# ================= RUTAS PERFIL (Con Control de Cobranza) =================

@app.route("/cliente/<int:cliente_id>", methods=["GET", "POST"])
@login_required
def perfil_cliente(cliente_id):
    try:
        if request.method == "POST":
            ft = request.form.get('form_type')
            if ft == 'seguimiento':
                tipo = request.form.get("tipo_interaccion")
                comentarios = request.form.get("comentarios")
                asesor = session.get('full_name')
                if tipo and comentarios:
                    db.crear_seguimiento(cliente_id, asesor, tipo, comentarios)
                    flash("Seguimiento registrado.", "success")
                else:
                    flash("Datos incompletos.", "error")
            elif ft == 'etiqueta':
                nombre = request.form.get('nombre_etiqueta', '').strip()
                if nombre:
                    eid = db.obtener_o_crear_etiqueta_id(nombre)
                    db.anadir_etiqueta_a_cliente(cliente_id, eid)
                    flash("Etiqueta añadida.", "success")
            
            return redirect(url_for('perfil_cliente', cliente_id=cliente_id))
        
        cliente = db.obtener_cliente_por_id(cliente_id)
        if not cliente:
            flash("Cliente no encontrado.", "error")
            return redirect(url_for('consulta'))
        
        return render_template("perfil_cliente.html", 
            cliente=cliente, 
            pagos=db.obtener_pagos_por_cliente(cliente_id),
            historial=db.obtener_seguimientos_por_cliente(cliente_id),
            etiquetas_cliente=db.obtener_etiquetas_por_cliente(cliente_id),
            current_section='crm')
    except DB_Error as e:
        flash(f"Error al cargar perfil: {e}", "error")
        return redirect(url_for('consulta'))

@app.route("/cliente/<int:cliente_id>/etiqueta/quitar/<int:etiqueta_id>")
@login_required
def quitar_etiqueta(cliente_id, etiqueta_id):
    try:
        db.quitar_etiqueta_a_cliente(cliente_id, etiqueta_id)
        flash("Etiqueta eliminada.", "success")
    except DB_Error as e:
        flash(f"Error: {e}", "error")
    return redirect(url_for('perfil_cliente', cliente_id=cliente_id))

@app.route("/seguimiento/atender/<int:seguimiento_id>", methods=["POST"])
@login_required
def atender_seguimiento(seguimiento_id):
    try:
        db.marcar_seguimiento_atendido(seguimiento_id)
        flash("Marcado como atendido.", "success")
    except DB_Error as e:
        flash(f"Error: {e}", "error")
    return redirect(url_for('perfil_cliente', cliente_id=request.form.get('cliente_id')))

@app.route("/seguimiento/eliminar/<int:seguimiento_id>", methods=["POST"])
@login_required
def eliminar_seguimiento(seguimiento_id):
    try:
        db.eliminar_seguimiento(seguimiento_id)
        flash("Seguimiento eliminado.", "success")
    except DB_Error as e:
        flash(f"Error: {e}", "error")
    return redirect(url_for('perfil_cliente', cliente_id=request.form.get('cliente_id')))

# ================= SECCIÓN GOOGLE SHEETS (ORIGINAL) =================

@app.route("/certificados")
@login_required
def certificados():
    q = request.args.get("query", "").strip().lower()
    page = request.args.get('page', 1, type=int)
    
    try:
        data = sheets_manager.obtener_datos_certificados()
        if q:
            data = [r for r in data if any(q in str(v).lower() for v in r.values())]
        
        total = len(data)
        pages = (total + RECORDS_PER_PAGE_SHEETS - 1) // RECORDS_PER_PAGE_SHEETS
        start = (page - 1) * RECORDS_PER_PAGE_SHEETS
        return render_template("certificados.html", certificados=data[start:start+RECORDS_PER_PAGE_SHEETS], page=page, total_pages=pages, query=q, is_certificate_section=True)
    except Exception as e:
        flash(f"Error certificados: {e}", "error")
        return render_template("certificados.html", certificados=[], page=1, total_pages=1, query=q, is_certificate_section=True)

@app.route("/diplomados")
@login_required
def diplomados():
    q = request.args.get("query", "").strip().lower()
    page = request.args.get('page', 1, type=int)
    
    try:
        data = sheets_manager.obtener_datos_diplomados()
        if q:
            data = [r for r in data if any(q in str(v).lower() for v in r.values())]
        
        total = len(data)
        pages = (total + RECORDS_PER_PAGE_SHEETS - 1) // RECORDS_PER_PAGE_SHEETS
        start = (page - 1) * RECORDS_PER_PAGE_SHEETS
        return render_template("diplomados.html", diplomados=data[start:start+RECORDS_PER_PAGE_SHEETS], page=page, total_pages=pages, query=q, is_certificate_section=True)
    except Exception as e:
        flash(f"Error diplomados: {e}", "error")
        return render_template("diplomados.html", diplomados=[], page=1, total_pages=1, query=q, is_certificate_section=True)

@app.route("/certificados/editar/<int:row_id>", methods=["GET", "POST"])
@login_required
def editar_certificado(row_id):
    if request.method=="POST":
        sheets_manager.actualizar_certificado(row_id, request.form.to_dict())
        return redirect(url_for('certificados'))
    try:
        data = next((d for d in sheets_manager.obtener_datos_certificados() if d['row_id']==row_id), None)
        if not data: return redirect(url_for('certificados'))
        return render_template("editar_certificado.html", registro={k:v for k,v in data.items() if k!='row_id'}, row_id=row_id)
    except: return redirect(url_for('certificados'))

@app.route("/diplomados/editar/<int:row_id>", methods=["GET", "POST"])
@login_required
def editar_diplomado(row_id):
    if request.method=="POST":
        sheets_manager.actualizar_diplomado(row_id, request.form.to_dict())
        return redirect(url_for('diplomados'))
    try:
        data = next((d for d in sheets_manager.obtener_datos_diplomados() if d['row_id']==row_id), None)
        if not data: return redirect(url_for('diplomados'))
        return render_template("editar_diplomado.html", registro={k:v for k,v in data.items() if k!='row_id'}, row_id=row_id)
    except: return redirect(url_for('diplomados'))

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static', 'images'), 'icon.png', mimetype='image/png')

# ================= SECCIÓN CONFIGURACIÓN DE METAS (Solo Admin) =================
@app.route('/configurar_metas', methods=['POST'])
@login_required
def configurar_metas():
    if session.get('role') != 'admin':
        return redirect(url_for('dashboard'))
             
    nueva_meta = request.form.get('meta_inscritos')
    
    # Validar que no sea vacío para evitar errores de SQL
    if not nueva_meta:
        nueva_meta = 0
        
    db.actualizar_metas(nueva_meta) # Ahora db_manager solo espera un argumento
         
    flash("Meta de inscritos actualizada correctamente.", "success")
    return redirect(url_for('dashboard'))


# Esta función se ha simplificado para reflejar que ahora solo se actualiza la meta de inscritos, eliminando cualquier referencia a la meta de dinero.
def actualizar_metas(inscritos):
    """Guarda únicamente la nueva meta de inscritos en la BD."""
    conn = db.get_connection()
    cursor = conn.cursor()
    try:
        # Eliminamos el campo meta_dinero de la consulta SQL
        cursor.execute("UPDATE metas_config SET meta_inscritos = %s WHERE id = 1", (inscritos,))
        conn.commit()
    except Exception as e:
        print(f"Error al actualizar metas: {e}")
    finally:
        conn.close()

# ================= SECCIÓN PRE-REGISTRO (Nueva Funcionalidad) =================
@app.route("/pre_registro")
@login_required
def pre_registro():
    return render_template("verificar_dni.html")

@app.route("/api/verificar_cliente/<dni>")
@login_required
def verificar_cliente_api(dni):
    # Usamos tu base de datos para buscar al cliente
    conn = db.get_connection()
    cursor = conn.cursor(dictionary=True)
    sql = """
        SELECT c.nombre, p.especialidad 
        FROM clientes c 
        LEFT JOIN pagos p ON c.id = p.cliente_id 
        WHERE c.dni = %s 
        ORDER BY p.id DESC LIMIT 1
    """
    cursor.execute(sql, (dni,))
    cliente = cursor.fetchone()
    conn.close()

    if cliente:
        return jsonify({
            "existe": True, 
            "nombre": cliente['nombre'], 
            "especialidad": cliente['especialidad'] or 'Sin asignar'
        })
    return jsonify({"existe": False})
