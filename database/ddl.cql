CREATE KEYSPACE sbd2_p1 
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};

USE sbd2_p1;

CREATE TABLE usuarios (
    dpi text PRIMARY KEY,
    nombre text,
    email text,
    telefono text,
    nit text
);

CREATE TABLE espacios (
    id uuid PRIMARY KEY,
    tipo text,
    nombre text,
    capacidad_maxima int,
    ubicacion text,
    estado_disponibilidad text
);

CREATE TABLE reservas (
    id uuid,
    dpi_usuario text,
    id_espacio uuid,
    fecha date,
    hora_inicio time,
    hora_fin time,
    duracion int,
    estado text,
    cantidad_asistentes int,
    nombre_reserva text,
    PRIMARY KEY ((dpi_usuario, fecha), id)
);

CREATE TABLE reservas_por_espacio (
    id_espacio uuid,
    fecha date,
    hora_inicio time,
    hora_fin time,
    id_reserva uuid,
    dpi_usuario text,
    estado text,
    PRIMARY KEY ((id_espacio, fecha), hora_inicio)
);
