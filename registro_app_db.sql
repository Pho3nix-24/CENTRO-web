-- 1. Preparación
CREATE DATABASE IF NOT EXISTS registro_app_db;
USE registro_app_db;

-- 2. Limpieza (En orden inverso de dependencia)
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS cliente_etiquetas, etiquetas, seguimientos, oportunidades, 
                   auditoria_accesos, pagos, clientes, metas_config;
SET FOREIGN_KEY_CHECKS = 1;

-- 3. Tabla Clientes
CREATE TABLE clientes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(255),
    dni VARCHAR(15) UNIQUE NOT NULL, 
    correo VARCHAR(255),
    celular VARCHAR(20),
    genero VARCHAR(20),
    estado VARCHAR(20) NOT NULL DEFAULT 'activo',
    estado_pago VARCHAR(20) DEFAULT 'AL DIA',
    curso_interes VARCHAR(100),
    asesor_asignado VARCHAR(255),
    fecha_contacto DATETIME
) ENGINE=InnoDB;

-- 4. Tabla Pagos
CREATE TABLE pagos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cliente_id INT NOT NULL,
    fecha DATETIME NOT NULL,
    cuota DECIMAL(10, 2) NOT NULL,
    tipo_de_cuota VARCHAR(50),
    banco VARCHAR(100),
    destino VARCHAR(100),
    numero_operacion VARCHAR(50) UNIQUE NOT NULL, 
    especialidad VARCHAR(100),
    modalidad VARCHAR(50),
    asesor VARCHAR(255),
    monto_total_diplomado DECIMAL(10, 2) DEFAULT 0.00,
    numero_cuota INT DEFAULT 1,
    proxima_fecha_pago DATE,
    FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
) ENGINE=InnoDB;

-- 5. Configuración de Metas
CREATE TABLE metas_config (
    id INT PRIMARY KEY,
    meta_inscritos INT DEFAULT 0,
    meta_dinero DECIMAL(10,2) DEFAULT 0.00
);
INSERT INTO metas_config (id, meta_inscritos, meta_dinero) VALUES (1, 500, 0.00);

-- 6. Auditoría y CRM
CREATE TABLE auditoria_accesos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME NOT NULL,
    usuario_app VARCHAR(255) NOT NULL,
    accion VARCHAR(50) NOT NULL,
    tabla_afectada VARCHAR(255),
    registro_id INT,
    detalles TEXT, 
    ip_origen VARCHAR(45)
);