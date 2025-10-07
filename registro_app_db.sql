CREATE DATABASE IF NOT EXISTS registro_app_db;
USE registro_app_db;

CREATE TABLE clientes (
    id INT AUTO_INCREMENT PRIMARY KEY,
    nombre VARCHAR(255) NOT NULL,
    dni VARCHAR(15) UNIQUE NOT NULL,
    correo VARCHAR(255) UNIQUE NOT NULL,
    celular VARCHAR(20),
    genero VARCHAR(20)
);

CREATE TABLE pagos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    cliente_id INT NOT NULL,
    fecha DATETIME NOT NULL,
    cuota DECIMAL(10, 2) NOT NULL,
    tipo_de_cuota VARCHAR(50),
    banco VARCHAR(100),
    destino VARCHAR(100),
    numero_operacion VARCHAR(50) UNIQUE,
    especialidad VARCHAR(100),
    modalidad VARCHAR(50),
    asesor VARCHAR(255),
    FOREIGN KEY (cliente_id) REFERENCES clientes(id) ON DELETE CASCADE
);

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
