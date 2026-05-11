# 🏛️ CENTRO - Ecosistema de Gestión Educativa & CRM

![GitHub Repo Size](https://img.shields.io/github/repo-size/alvaro-lopez/CENTRO-web?color=blue&logo=github)
![Flask Version](https://img.shields.io/badge/Flask-3.1.2-white?logo=flask&logoColor=black)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)
![Database](https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql&logoColor=white)
![Status](https://img.shields.io/badge/Status-Activo-success)

## 📖 Descripción General

**CENTRO** no es solo un gestor de pagos; es una solución **Full-Stack** diseñada para digitalizar y optimizar la operatividad de centros de formación académica. El sistema integra el flujo comercial completo: desde que un interesado llega (Lead), pasando por el embudo de ventas del **CRM**, hasta la automatización de cobranzas y la entrega de certificaciones vinculadas a la nube.

Esta plataforma elimina el error humano en las cuentas por cobrar y proporciona al equipo administrativo una visión clara y en tiempo real del crecimiento de la institución.

---

## 🚀 Módulos y Funcionalidades

### 📈 Smart Dashboard & Analítica
* **Seguimiento de Metas:** Control visual de inscritos frente a objetivos mensuales.
* **Ingresos en Tiempo Real:** Cálculo automático de recaudación y proyecciones de deuda.
* **KPIs de Conversión:** Análisis de efectividad del equipo de ventas.

### 💼 CRM de Alta Conversión
* **Pipeline de Ventas:** Gestión dinámica de oportunidades (Nuevo, Contactado, Negociación, Cerrado).
* **Gestión de Leads:** Registro detallado de prospectos con asignación inteligente a asesores.
* **Asignación de Roles:** Permisos granulares para agentes de ventas, atención al cliente y administradores.

### 💳 Gestión Financiera Automatizada
* **Cálculo de Cuotas:** El sistema determina automáticamente si un alumno está "Al día" o tiene "Deuda".
* **Historial de Transacciones:** Registro con soporte de imágenes para comprobantes de pago.
* **Auditoría de Movimientos:** Trazabilidad completa de quién, cuándo y qué se modificó en la base de datos.

### ☁️ Cloud Sync (Google Workspace)
* **Certificados Digitales:** Conexión directa con Google Sheets para gestionar folios y certificados.
* **Caché Inteligente:** Optimización de peticiones a la API para un rendimiento fluido.
* **Visor de Datos Externos:** Acceso a diplomados y cursos registrados en hojas de cálculo directamente desde la web.

---

## 🛠️ Stack Tecnológico

* **Core:** Python 3.10 con Flask Framework.
* **Frontend:** Arquitectura basada en Jinja2, CSS3 (Variables modernas) y componentes JavaScript.
* **Almacenamiento:** MySQL con lógica de procedimientos almacenados y disparadores.
* **Seguridad:** Encriptación de contraseñas con Scrypt y Rate-Limiting para prevención de intrusos.

---

## ⚙️ Configuración del Entorno

1.  **Clonación y Requerimientos:**
    ```bash
    git clone https://github.com/tu-usuario/CENTRO-web.git
    pip install -r requirements.txt
    ```
2.  **Base de Datos:** Ejecuta el script `registro_app_db.sql` en tu instancia de MySQL.
3.  **Variables de Entorno:** Configura las credenciales de Google API en `credentials.json`.
4.  **Despliegue:**
    ```bash
    python run.py
    ```

---

## 🛡️ Control de Acceso (RBAC)
El sistema utiliza una política de privilegios mínimos:
* **Admin:** Acceso total.
* **Equipo:** Gestión de pagos y reportes básicos.
* **CRM:** Foco exclusivo en ventas y leads.
* **Atención al Cliente:** Solo consultas y verificación de estados.

---
*Desarrollado con precisión técnica por Alvaro L.O (Pho3nix-24).*
