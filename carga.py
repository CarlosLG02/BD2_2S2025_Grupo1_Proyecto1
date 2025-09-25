from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime, timedelta
import uuid
import random
from tqdm import tqdm

# Conectar al cluster
cluster = Cluster(contact_points=["127.0.0.1"], port=9042)
session = cluster.connect("sbd2_p1")

# 1. Insertar usuarios
usuarios = []
for i in range(1, 21):
    dpi = f"DPI{i:04d}"
    nombre = f"Usuario{i}"
    email = f"usuario{i}@example.com"
    telefono = f"{random.randint(10000000, 99999999)}"
    nit = f"NIT-{random.randint(100000, 999999)}"
    usuarios.append(dpi)
    session.execute(
        "INSERT INTO usuario (dpi, nombre, email, telefono, nit) VALUES (%s, %s, %s, %s, %s)",
        (dpi, nombre, email, telefono, nit)
    )

# 2. Insertar espacios
espacios = []
tipos = ["salon1", "salon2", "auditorio1",
         "francisco_vela", "iglu"]  # salones
for j in range(1, 6):
    id_esp = f"ESP{j:02d}"
    nombre = f"Espacio {j}"
    tipo = tipos[j-1]
    capacidad = random.choice([10, 20, 50, 100])
    ubicacion = f"Nivel {random.randint(1, 5)}"
    espacios.append(id_esp)
    session.execute(
        "INSERT INTO espacio (id_espacio, nombre, tipo, capacidad_maxima, ubicacion) VALUES (%s, %s, %s, %s, %s)",
        (id_esp, nombre, tipo, capacidad, ubicacion)
    )

# 3. Insretar 100,000 reservas
fecha_inicio = datetime(2025, 1, 1)
dias_rango = 365  # un año para el rango de fechas
for n in tqdm(range(100000), desc="Insertando reservas"):

    # usuario y espacio aleatoriamente
    dpi = random.choice(usuarios)
    id_esp = random.choice(espacios)

    # fecha y hora aleatoria
    delta_dias = random.randint(0, dias_rango - 1)
    fecha_reserva = fecha_inicio + timedelta(days=delta_dias)

    # hora de inicio entre 0 y 23h
    hora = random.randint(0, 23)

    # slots de comienzo
    minuto = random.choice([0, 30])
    inicio = fecha_reserva.replace(
        hour=hora, minute=minuto, second=0, microsecond=0)

    # Duración entre 1 a 3 horas
    duracion_horas = random.randint(1, 3)
    fin = inicio + timedelta(hours=duracion_horas)

    # UUID para la reserva
    id_reserva = uuid.uuid4()
    estado = "activa"

    # Obtener detalles del usuario y espacio
    nombre_usuario = f"Usuario{dpi[-4:]}"    # formato DPI0001 -> Usuario1

    nombre_espacio = f"Espacio {int(id_esp[-2:])}"
    tipo_espacio = tipos[int(id_esp[-2:]) - 1]

    # usamos la lista creada para capacidda y ubicaion:
    capacidad_espacio = None  # cambiar none
    ubicacion_espacio = None  # cambiar none

    # preparasrese para insertar
    batch_query = """
    BEGIN BATCH
    INSERT INTO reservas_por_usuario (dpi, fecha, hora_inicio, id_reserva, id_espacio, nombre_espacio, tipo_espacio, ubicacion_espacio, hora_fin, estado, nombre_usuario, capacidad_espacio)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    INSERT INTO reservas_por_espacio (id_espacio, fecha, hora_inicio, id_reserva, dpi, nombre_usuario, hora_fin, estado, tipo_espacio, capacidad_espacio, ubicacion_espacio)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    INSERT INTO reservas_por_fecha (fecha, hora_inicio, id_espacio, id_reserva)
    VALUES (%s, %s, %s, %s);
    APPLY BATCH;
    """

    params = (
        dpi, inicio.date(), inicio.time(), id_reserva, id_esp, nombre_espacio, tipo_espacio, ubicacion_espacio, fin.time(
        ), estado, nombre_usuario, capacidad_espacio,
        id_esp, inicio.date(), inicio.time(), id_reserva, dpi, nombre_usuario, fin.time(
        ), estado, tipo_espacio, capacidad_espacio, ubicacion_espacio,
        inicio.date(), inicio.time(), id_esp, id_reserva
    )

    session.execute(batch_query, params)
