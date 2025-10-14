"""
Módulo de Rutas de Flask
-----------------------
Este archivo define todas las URLs de la aplicación, la lógica de negocio,
y el control de acceso basado en roles y límite de intentos de login.
"""

# --- Importaciones ---
import os
from datetime import datetime
from functools import wraps
from flask import (
    render_template, request, redirect, url_for,
    flash, session, send_file, send_from_directory
)
from app import app
from app import database_manager as db
from mysql.connector import IntegrityError, Error as DB_Error
from werkzeug.security import check_password_hash
from app import sheets_manager


# --- Configuración y Constantes ---
RECORDS_PER_PAGE = 5

# --- Configuración de Seguridad de Login ---
failed_logins = {}  # Diccionario en memoria para rastrear fallos por IP
LOGIN_ATTEMPT_LIMIT = 5  # Intentos máximos antes de bloquear
LOCKOUT_TIME_SECONDS = 300  # 5 minutos de bloqueo

# Diccionario de usuarios con roles definidos
USERS = {
    'admin':      {'password_hash': 'scrypt:32768:8:1$WFi0YBN2qCDwBgWJ$be6ca3584230b85b3b4fdbba30a11bf25d0b30fd525ac6c1a83708edf7401d9300ab2e3388eee7835ce47534dbfcdb4458b538db90ec960ef3852f793c869b47', 'full_name': 'Administrador',   'role': 'admin'},
    'lud_rojas':  {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72',       'full_name': 'Lud Rojas',       'role': 'equipo'},
    'ruth_lecca': {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72',       'full_name': 'Ruth Lecca',      'role': 'equipo'},
    'rafa_diaz':  {'password_hash': 'scrypt:32768:8:1$4Dc8HIUCPu0LkrYZ$60b205b9a1f98687a869cd0905efd50c02a4be5ad34b0e2954fa140f0a56317da4c4b1748e5fc87b983d032d915439f7c60a37d617d977452bf5b4de0aca1d72', 'full_name': 'Rafael Díaz',     'role': 'atencion_cliente'}
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

RECORDS_PER_PAGE_SHEETS = 20

# --- Funciones Auxiliares ---
def get_user_ip():
    """Obtiene la IP real del usuario, incluso detrás de un proxy como Nginx."""
    if request.headers.getlist("X-Forwarded-For"):
        return request.headers.getlist("X-Forwarded-For")[0]
    return request.remote_addr

# Decorador para proteger rutas que requieren login
def login_required(f):
    """Decorador para proteger rutas, asegurando que el usuario haya iniciado sesión."""
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
    """
    Maneja el inicio de sesión de usuarios, usando hasheo de contraseñas
    y un sistema de bloqueo por intentos fallidos para prevenir ataques de fuerza bruta.
    """
    ip_usuario = get_user_ip()

    # 1. VERIFICAR SI LA IP ESTÁ BLOQUEADA
    if ip_usuario in failed_logins:
        user_failures = failed_logins[ip_usuario]
        elapsed_time = (datetime.now() - user_failures['last_attempt_time']).total_seconds()

        if user_failures['attempts'] >= LOGIN_ATTEMPT_LIMIT and elapsed_time < LOCKOUT_TIME_SECONDS:
            tiempo_restante = round((LOCKOUT_TIME_SECONDS - elapsed_time) / 60)
            flash(f"Demasiados intentos fallidos. Por favor, espera {tiempo_restante} minutos.", "error")
            return render_template("login.html")
        
        if elapsed_time >= LOCKOUT_TIME_SECONDS:
            failed_logins.pop(ip_usuario, None)

    # 2. PROCESAR EL INTENTO DE LOGIN
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password") # Contraseña en texto plano que envía el usuario
        user_data = USERS.get(username)
        
        # --- CAMBIO CLAVE: CÓMO SE VERIFICA LA CONTRASEÑA ---
        if user_data and check_password_hash(user_data['password_hash'], password):
            # --- LOGIN EXITOSO ---
            failed_logins.pop(ip_usuario, None)  # Limpia el registro de fallos
            
            session['logged_in'] = True
            session['full_name'] = user_data['full_name']
            session['username'] = username
            session['role'] = user_data['role']
            
            db.registrar_auditoria(user_data['full_name'], "INICIO_SESION_EXITOSO", ip_usuario)
            flash("Has iniciado sesión correctamente.", "success")
            return redirect(url_for("menu"))
        else:
            # --- LOGIN FALLIDO ---
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
    """Cierra la sesión del usuario y registra el evento."""
    usuario_actual = session.get('full_name', 'desconocido')
    ip_usuario = get_user_ip()
    db.registrar_auditoria(usuario_actual, "CIERRE_SESION", ip_usuario)

    session.clear()
    flash("Has cerrado la sesión.", "success")
    return redirect(url_for('login'))

# --- Ruta del Menú Principal ---
@app.route("/menu")
@login_required
def menu():
    """Muestra el menú principal para elegir la sección."""
    return render_template("menu.html")

# --- Rutas Principales de la Aplicación ---

# --- Ruta del Dashboard ---
@app.route("/dashboard")
@login_required
def dashboard():
    """Muestra el panel principal con estadísticas y registros recientes."""
    # Redirige a los de atención al cliente a su única página permitida
    if session.get('role') == 'atencion_cliente':
        return redirect(url_for('consulta'))

    try:
        estadisticas = db.obtener_estadisticas_dashboard()
        ultimos_pagos = db.obtener_ultimos_pagos(5)
    except DB_Error as e:
        flash(f"Error al cargar los datos del dashboard: {e}", "error")
        estadisticas = {'registros_hoy': 0, 'ingresos_hoy': 0, 'ingresos_mes': 0}
        ultimos_pagos = []

    return render_template(
        "dashboard.html", 
        stats=estadisticas, 
        pagos=ultimos_pagos
    )

# Rutas de Gestión de Pagos y Clientes ---
@app.route("/")
@login_required
def index():
    """Página principal (Dashboard). Muestra estadísticas y registros recientes."""
    if session.get('role') == 'atencion_cliente':
        return redirect(url_for('consulta'))
    try:
        estadisticas = db.obtener_estadisticas_dashboard()
        ultimos_pagos = db.obtener_ultimos_pagos(5)
    except DB_Error as e:
        flash(f"Error al cargar los datos del dashboard: {e}", "error")
        estadisticas = {'registros_hoy': 0, 'ingresos_hoy': 0, 'ingresos_mes': 0}
        ultimos_pagos = []
    return render_template("dashboard.html", stats=estadisticas, pagos=ultimos_pagos)

# --- Rutas de Gestión de Pagos y Clientes ---
@app.route("/registrar", methods=['GET'])
@login_required
def registrar():
    """Muestra la página con el formulario de registro."""
    if session.get('role') == 'atencion_cliente':
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))
    return render_template("index.html")

# --- Manejo de Formularios y Operaciones CRUD ---
@app.route("/submit", methods=["POST"])
@login_required
def submit():
    """Procesa los datos del formulario de nuevo registro."""
    if session.get('role') == 'atencion_cliente':
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))
        
    form_data = request.form.to_dict()
    try:
        # (Validación de fecha)
        fecha_str = form_data.get('fecha')
        if not fecha_str:
            flash("La fecha es un campo obligatorio.", "error")
            return redirect(url_for("registrar")) # Redirige al formulario de registro
        try:
            form_data['fecha'] = datetime.strptime(fecha_str, '%Y-%m-%d')
        except ValueError:
            flash("El formato de la fecha es incorrecto. Por favor, usa AAAA-MM-DD.", "error")
            return redirect(url_for("registrar")) # Redirige al formulario de registro
        
        # (Lógica de base de datos)
        cliente_id = db.buscar_o_crear_cliente(form_data)
        form_data['numero_operacion'] = form_data.pop('num_operacion', None)
        
        nuevo_pago_id = db.crear_pago(cliente_id, form_data)
        flash("Registro guardado correctamente.", "success")

        # (Auditoría)
        usuario_actual = session.get('full_name', 'desconocido')
        ip_usuario = get_user_ip()
        detalles = f"Cliente ID: {cliente_id}, Pago ID: {nuevo_pago_id}"
        db.registrar_auditoria(usuario_actual, "CREAR_PAGO", ip_usuario, "pagos", nuevo_pago_id, detalles)

    except IntegrityError:
        flash("Error: El DNI, correo o N° de Operación ingresado ya existe en otro registro.", "error")
    except DB_Error as e:
        flash(f"Error al guardar el registro: {e}", "error")
    
    # Al terminar, redirige de vuelta al formulario de registro
    return redirect(url_for("registrar"))

# Página de consulta con paginación
@app.route("/consulta", methods=["GET"])
@login_required
def consulta():
    """Muestra la página de consulta y los resultados de búsqueda con paginación."""
    query = request.args.get("query", "").strip().lower()
    page = request.args.get('page', 1, type=int)
    
    resultados_paginados = []
    total_pages = 1
    
    try:
        # La función buscar_pagos_completos con un query vacío devolverá todos los registros.
        todos_los_resultados = db.buscar_pagos_completos(query)
        
        # Aplicamos la paginación a los resultados (sean todos o filtrados)
        total_records = len(todos_los_resultados)
        total_pages = (total_records + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE if RECORDS_PER_PAGE > 0 else 1
        start_index = (page - 1) * RECORDS_PER_PAGE
        end_index = start_index + RECORDS_PER_PAGE
        resultados_paginados = todos_los_resultados[start_index:end_index]
        
    except DB_Error as e:
        flash(f"Error al consultar la base de datos: {e}", "error")
        
    headers_db = ["ID"] + HEADERS
    return render_template(
        "consulta.html", 
        resultados=resultados_paginados, 
        headers=headers_db,
        page=page,
        total_pages=total_pages,
        query=query  # Pasamos el query para mantenerlo en los enlaces de paginación
    )
    
# Edición de un pago existente
@app.route("/editar/<int:id>", methods=["GET", "POST"])
@login_required
def editar(id):
    """Edición de un pago. Accesible para todos los roles."""
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
    labels_and_fields = list(zip(HEADERS, FIELDS))
    return render_template("editar.html", data=data, labels_and_fields=labels_and_fields, id=id, query=request.args.get('query', ''))

# Renovación de pago (crear un nuevo pago basado en uno existente)
@app.route("/actualizar_pago/<int:id>", methods=["GET", "POST"])
@login_required
def actualizar_pago(id):
    """Renovación de pago. Restringido a admin y equipo."""
    if session.get('role') == 'atencion_cliente':
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))

    if request.method == "POST":
        # Obtenemos el 'query' para mantener el filtro en la redirección
        query = request.form.get("query", "")
        try:
            pago_original = db.obtener_pago_por_id(id)
            if not pago_original:
                flash("Error: No se encontró el registro original.", "error")
                return redirect(url_for("consulta", query=query))

            cliente_id = pago_original['cliente_id']
            datos_nuevo_pago = request.form.to_dict()
            datos_nuevo_pago['fecha'] = datetime.now()
            datos_nuevo_pago['numero_operacion'] = datos_nuevo_pago.pop('num_operacion', None)
            
            # Completamos datos que no vienen en el formulario simple de pago
            datos_nuevo_pago['especialidad'] = pago_original['especialidad']
            datos_nuevo_pago['modalidad'] = pago_original['modalidad']
            datos_nuevo_pago['asesor'] = pago_original['asesor']
            
            nuevo_pago_id = db.crear_pago(cliente_id, datos_nuevo_pago)
            flash("Renovación de pago registrada exitosamente.", "success")
            
            # Auditoría
            usuario_actual = session.get('full_name', 'desconocido')
            ip_usuario = get_user_ip()
            detalles = f"Cliente ID: {cliente_id}, Pago ID: {nuevo_pago_id} (RENOVACIÓN)"
            db.registrar_auditoria(usuario_actual, "RENOVAR_PAGO", ip_usuario, "pagos", nuevo_pago_id, detalles)

            return redirect(url_for("consulta", query=query))

        except IntegrityError:
            # Captura el error de N° de Operación duplicado
            flash("Error: El N° de Operación ingresado ya existe en otro registro. Por favor, verifícalo.", "error")
            # Devuelve al usuario al mismo formulario para que lo corrija
            return redirect(url_for("actualizar_pago", id=id, query=query))

        except DB_Error as e:
            # Captura cualquier otro error de la base de datos
            flash(f"Error al procesar el pago: {e}", "error")
            return redirect(url_for("consulta", query=query))
    
    # Esto se ejecuta cuando el método es GET (la primera vez que se carga la página)
    query = request.args.get('query', '')
    datos_pago_actual = db.obtener_pago_por_id(id)
    return render_template("actualizar_pago.html", data=datos_pago_actual, id=id, query=query)

#   Eliminar un cliente (borrado lógico)
@app.route("/desactivar_cliente", methods=["POST"])
@login_required
def desactivar_cliente():
    """Desactiva un cliente (borrado lógico). Restringido a admin y equipo."""
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
            flash("Cliente desactivado correctamente. Ya no aparecerá en las búsquedas.", "success")

            usuario_actual = session.get('full_name', 'desconocido')
            ip_usuario = get_user_ip()
            db.registrar_auditoria(usuario_actual, "DESACTIVAR_CLIENTE", ip_usuario, "clientes", cliente_id)
        else:
            flash("Error: No se encontró el pago asociado.", "error")
    except DB_Error as e:
        flash(f"Error: No se pudo desactivar al cliente: {e}", "error")
    return redirect(url_for("consulta", query=query))


# Reactivar un cliente (cambiar su estado a 'activo')
@app.route("/reactivar_cliente", methods=["POST"])
@login_required
def reactivar_cliente():
    """Reactiva un cliente (cambia su estado a 'activo')."""
    if session.get('role') not in ['admin', 'equipo']:
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))

    # Para reactivar, necesitamos el ID del CLIENTE, no del pago.
    # La forma más fácil es pasarlo desde el formulario.
    cliente_id = int(request.form.get("cliente_id"))
    query = request.form.get("query", "")
    try:
        db.cambiar_estado_cliente(cliente_id, 'activo')
        flash("Cliente reactivado correctamente.", "success")

        # Auditoría
        usuario_actual = session.get('full_name', 'desconocido')
        ip_usuario = get_user_ip()
        db.registrar_auditoria(usuario_actual, "REACTIVAR_CLIENTE", ip_usuario, "clientes", cliente_id)
    except DB_Error as e:
        flash(f"Error al reactivar al cliente: {e}", "error")
        
    return redirect(url_for("consulta", query=query))

# Eliminar un PAGO (borrado físico) - SOLO ADMIN
@app.route("/eliminar_pago", methods=["POST"])
@login_required
def eliminar_pago():
    """Elimina un registro de pago de forma permanente. Admin y equipo."""
    if session.get('role') not in ['admin', 'equipo']:
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))

    pago_id = int(request.form.get("id"))
    query = request.form.get("query", "")
    try:
        # Usamos la función que ya existía en database_manager.py
        filas_afectadas = db.eliminar_pago(pago_id)
        if filas_afectadas > 0:
            flash(f"Registro de pago ID: {pago_id} eliminado permanentemente.", "success")
            
            # Auditoría
            usuario_actual = session.get('full_name', 'desconocido')
            ip_usuario = get_user_ip()
            db.registrar_auditoria(usuario_actual, "ELIMINAR_PAGO_PERMANENTE", ip_usuario, "pagos", pago_id)
        else:
            flash("Error: No se encontró el registro de pago para eliminar.", "error")
            
    except DB_Error as e:
        flash(f"Error de base de datos al eliminar el pago: {e}", "error")
        
    return redirect(url_for("consulta", query=query))

# --- Rutas de Reportes y Administración ---

@app.route("/reportes")
@login_required
def reportes():
    """Página de reportes. Restringida a admin y equipo."""
    if session.get('role') == 'atencion_cliente':
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))
    # ... (el resto del código de la función se queda igual) ...
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
            page_total_registros=page_total_registros
        )
    except DB_Error as e:
        flash(f"Error al generar el reporte: {e}", "error")
        return render_template("reportes.html", reporte=[])


@app.route("/descargar")
@login_required
def descargar():
    """Descarga de Excel. Restringida a admin y equipo."""
    if session.get('role') == 'atencion_cliente':
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('consulta'))
    # ... (el resto del código de la función se queda igual) ...
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

# --- Rutas de Auditoría ---
@app.route("/auditoria")
@login_required
def auditoria():
    """Muestra el log de auditoría completo (solo para administradores)."""
    if session.get('username') != 'admin':
        flash("Acceso no autorizado.", "error")
        return redirect(url_for('index'))

    try:
        # Simplemente obtenemos todos los logs y los pasamos directamente
        todos_los_logs = db.leer_log_auditoria()
        return render_template("auditoria.html", logs=todos_los_logs)
        
    except DB_Error as e:
        flash(f"Error al leer la auditoría: {e}", "error")
        return render_template("auditoria.html", logs=[])

# --- Rutas de Perfil de Cliente ---    
@app.route("/cliente/<int:cliente_id>")
@login_required
def perfil_cliente(cliente_id):
    """Muestra el perfil de un cliente y su historial de pagos."""
    try:
        cliente = db.obtener_cliente_por_id(cliente_id)
        pagos = db.obtener_pagos_por_cliente(cliente_id)
        
        if not cliente:
            flash("Error: Cliente no encontrado.", "error")
            return redirect(url_for('consulta'))
            
        return render_template("perfil_cliente.html", cliente=cliente, pagos=pagos)
    except DB_Error as e:
        flash(f"Error al cargar el perfil del cliente: {e}", "error")
        return redirect(url_for('consulta'))

# --- Rutas de Integración con Google Sheets ---    
@app.route("/certificados")
@login_required
def certificados():
    """
    Muestra los datos de certificados con búsqueda y paginación.
    """
    # 1. Obtener parámetros de la URL
    query = request.args.get("query", "").strip().lower()
    page = request.args.get('page', 1, type=int)
    
    # 2. Cargar todos los datos (usará el caché que implementamos)
    todos_los_datos = sheets_manager.obtener_datos_certificados()
    
    # 3. Filtrar los resultados si hay una búsqueda
    resultados_filtrados = []
    if query:
        for registro in todos_los_datos:
            # Convierte todos los valores del registro a string y minúsculas para buscar
            if any(query in str(value).lower() for value in registro.values()):
                resultados_filtrados.append(registro)
    else:
        resultados_filtrados = todos_los_datos
        
    # 4. Aplicar la paginación a los resultados filtrados
    total_records = len(resultados_filtrados)
    total_pages = (total_records + RECORDS_PER_PAGE_SHEETS - 1) // RECORDS_PER_PAGE_SHEETS
    start_index = (page - 1) * RECORDS_PER_PAGE_SHEETS
    end_index = start_index + RECORDS_PER_PAGE_SHEETS
    
    resultados_paginados = resultados_filtrados[start_index:end_index]
    
    return render_template(
        "certificados.html", 
        certificados=resultados_paginados,
        page=page,
        total_pages=total_pages,
        query=query,
        is_certificate_section=True
    )

# --- Nueva Ruta para Editar Registros en Google Sheets ---   
@app.route("/certificados/editar/<int:row_id>", methods=["GET", "POST"])
@login_required
def editar_certificado(row_id):
    """
    Maneja la edición de un registro de certificado.
    """
    # Buscamos el registro específico que el usuario quiere editar
    todos_los_datos = sheets_manager.obtener_datos_certificados()
    registro_a_editar = next((item for item in todos_los_datos if item['row_id'] == row_id), None)

    if not registro_a_editar:
        flash("Error: No se encontró el registro para editar.", "error")
        return redirect(url_for('certificados'))

    if request.method == "POST":
        # El usuario ha enviado el formulario con los cambios
        form_data = request.form.to_dict()
        try:
            sheets_manager.actualizar_certificado(row_id, form_data)
            flash("Registro actualizado correctamente en Google Sheets.", "success")
            return redirect(url_for('certificados'))
        except Exception as e:
            flash(f"Error al actualizar el registro: {e}", "error")

    # Si es GET, mostramos el formulario de edición con los datos actuales
    # Excluimos 'row_id' de los campos a mostrar en el formulario
    campos_editables = {k: v for k, v in registro_a_editar.items() if k != 'row_id'}
    return render_template("editar_certificado.html", registro=campos_editables, row_id=row_id)

# --- Ruta para la Página de Diplomados ---
@app.route("/diplomados")
@login_required
def diplomados():
    """Muestra los datos de la hoja de cálculo de diplomados."""
    query = request.args.get("query", "").strip().lower()
    page = request.args.get('page', 1, type=int)
    
    todos_los_datos = sheets_manager.obtener_datos_diplomados()
    
    resultados_filtrados = []
    if query:
        for registro in todos_los_datos:
            if any(query in str(value).lower() for value in registro.values()):
                resultados_filtrados.append(registro)
    else:
        resultados_filtrados = todos_los_datos
        
    total_records = len(resultados_filtrados)
    total_pages = (total_records + RECORDS_PER_PAGE_SHEETS - 1) // RECORDS_PER_PAGE_SHEETS
    start_index = (page - 1) * RECORDS_PER_PAGE_SHEETS
    end_index = start_index + RECORDS_PER_PAGE_SHEETS
    resultados_paginados = resultados_filtrados[start_index:end_index]
    
    return render_template(
        "diplomados.html", 
        diplomados=resultados_paginados,
        page=page,
        total_pages=total_pages,
        query=query,
        is_certificate_section=True
    )

# --- Ruta para Editar Registros de Diplomados en Google Sheets ---
@app.route("/diplomados/editar/<int:row_id>", methods=["GET", "POST"])
@login_required
def editar_diplomado(row_id):
    """Maneja la edición de un registro de diplomado."""
    todos_los_datos = sheets_manager.obtener_datos_diplomados()
    registro_a_editar = next((item for item in todos_los_datos if item['row_id'] == row_id), None)

    if not registro_a_editar:
        flash("Error: No se encontró el registro para editar.", "error")
        return redirect(url_for('diplomados'))

    if request.method == "POST":
        form_data = request.form.to_dict()
        try:
            sheets_manager.actualizar_diplomado(row_id, form_data)
            flash("Registro de diplomado actualizado correctamente.", "success")
            return redirect(url_for('diplomados'))
        except Exception as e:
            flash(f"Error al actualizar el registro: {e}", "error")

    campos_editables = {k: v for k, v in registro_a_editar.items() if k != 'row_id'}
    return render_template("editar_diplomado.html", registro=campos_editables, row_id=row_id)

# --- Ruta del Favicon ---
@app.route('/favicon.ico')
def favicon():
    """Sirve el ícono de la aplicación."""
    return send_from_directory(os.path.join(app.root_path, 'static', 'images'), 'icon.png', mimetype='image/png')