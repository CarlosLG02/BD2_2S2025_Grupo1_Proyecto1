CREATE KEYSPACE sbd2_p1 
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};

USE sbd2_p1;

CREATE TABLE usuario (
    dpi text PRIMARY KEY,
    nombre text,
    email text,
    telefono text,
    nit text
);

CREATE TABLE espacio (
    id_espacio TEXT PRIMARY KEY,
    tipo text,
    nombre text,
    ubicacion text,
    capacidad_maxima int,
);

-- Historial de reservas por Usuario
CREATE TABLE reservas_por_usuario (
  dpi TEXT,                
  fecha DATE,               
  hora_inicio TIME,   
  hora_fin TIME,      
  id_reserva UUID,          
  id_espacio TEXT,
  nombre_espacio TEXT,
  tipo_espacio TEXT,
  ubicacion_espacio TEXT,
  estado TEXT,
  nombre_usuario TEXT,
  capacidad_espacio INT,
  PRIMARY KEY ((dpi), fecha, hora_inicio, id_reserva)
) WITH CLUSTERING ORDER BY (fecha DESC, hora_inicio ASC, id_reserva ASC);

-- Historial de reservas por Espacio (fecha/hora)
CREATE TABLE reservas_por_espacio (
  id_espacio TEXT,          
  fecha DATE,               
  hora_inicio TIME,     
  hora_fin TIME,    
  id_reserva UUID,          
  dpi TEXT,
  nombre_usuario TEXT,
  tipo_espacio TEXT,
  capacidad_espacio INT,
  ubicacion_espacio TEXT,
  estado TEXT,
  PRIMARY KEY ((id_espacio), fecha, hora_inicio, id_reserva)
) WITH CLUSTERING ORDER BY (fecha DESC, hora_inicio ASC, id_reserva ASC);

-- Disponibilidad de espacios por fecha y hora
CREATE TABLE reservas_por_fecha (
  fecha DATE,             
  hora_inicio TIME,         
  id_espacio TEXT,          
  id_reserva UUID,
  PRIMARY KEY ((fecha), hora_inicio, id_espacio, id_reserva)
);
