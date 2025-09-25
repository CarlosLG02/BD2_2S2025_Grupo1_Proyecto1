from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
import uuid
from collections import defaultdict
from datetime import date, datetime, time, timedelta


cluster = Cluster(contact_points=["127.0.0.1"], port=9042)
session = cluster.connect("sbd2_p1")

# consulta espacios disponibles por fecha


def esapcios_disponibles(fecha):

    filas = session.execute(
        "SELECT id_espacio FROM reservas_por_fecha WHERE fecha=%s",
        (fecha,)
    )
    espacios_ocupados = {row.id_espacio for row in filas}

    # Obtener todos los espacios
    todos = {row.id_espacio for row in session.execute(
        "SELECT id_espacio FROM espacio")}
    libres = list(todos - espacios_ocupados)

    print(f"Espacios disponibles el {fecha}: {libres}")
    return libres


# reservas de un espacio en rango de fechas, año, mes,dia


def historial_reservas_usuario(dpi, fecha_inicio=None, fecha_fin=None):
    if fecha_inicio and fecha_fin:

        query = session.prepare("""
            SELECT fecha, hora_inicio, hora_fin, id_espacio, estado, nombre_espacio 
            FROM reservas_por_usuario 
            WHERE dpi=? AND fecha >= ? AND fecha <= ?
        """)
        params = (dpi, fecha_inicio, fecha_fin)
    else:
        query = session.prepare("""
            SELECT fecha, hora_inicio, hora_fin, id_espacio, estado, nombre_espacio 
            FROM reservas_por_usuario 
            WHERE dpi=?
        """)
        params = (dpi,)

    rows = session.execute(query, params)

    print(f"\nHistorial de reservas del usuario {dpi}:")
    for row in rows:
        print(f" - {row.fecha} | {row.hora_inicio}-{row.hora_fin} | {row.nombre_espacio} ({row.id_espacio}) | Estado: {row.estado}")


# Historial de reservas de un espacio en rango de fechas
def get_espacios():
    """
    Devuelve la lista de IDs de todos los espacios registrados.
    """
    rows = session.execute("SELECT id_espacio FROM espacio")
    return [row.id_espacio for row in rows]

# reservas de un espacio en rango de fechas


def ocupacion_por_fechas(fecha_inicio, fecha_fin):
    espacios = get_espacios()
    ocupacion = defaultdict(list)
    for espacio in espacios:
        query = session.prepare("""
            SELECT fecha, hora_inicio, hora_fin, dpi, estado 
            FROM reservas_por_espacio 
            WHERE id_espacio=? AND fecha >= ? AND fecha <= ?
        """)
        rows = session.execute(query, (espacio, fecha_inicio, fecha_fin))
        for row in rows:
            ocupacion[espacio].append({
                'fecha': row.fecha,
                'hora_inicio': row.hora_inicio,
                'hora_fin': row.hora_fin,
                'dpi': row.dpi,
                'estado': row.estado
            })

    print(f"\nOcupación de espacios del {fecha_inicio} al {fecha_fin}:")
    for espacio_id, reservas in ocupacion.items():
        print(f"Espacio {espacio_id} - {len(reservas)} reservas:")
        for r in reservas:
            print(
                f"   - {r['fecha']} | {r['hora_inicio']}-{r['hora_fin']} | Usuario: {r['dpi']} | Estado: {r['estado']}")


# Verificar que los datos se cargaron correctamente
def verificar_datos():
    print("=== VERIFICACIÓN DE DATOS ===")

    # Contar usuarios
    usuarios = session.execute("SELECT COUNT(*) as total FROM usuario")
    print(f"Total usuarios: {usuarios.one().total}")

    # Contar espacios
    espacios = session.execute("SELECT COUNT(*) as total FROM espacio")
    print(f"Total espacios: {espacios.one().total}")

    # Contar reservas (aproximado)
    reservas = session.execute(
        "SELECT COUNT(*) as total FROM reservas_por_usuario LIMIT 1000")
    print(f"Reservas en tabla de usuarios: {reservas.one().total}")


def espacios_ocupados(fecha):
    """
    Versión alternativa usando reservas_por_fecha (sin hora_fin)
    """
    filas = session.execute(
        "SELECT id_espacio, hora_inicio FROM reservas_por_fecha WHERE fecha=%s",
        (fecha,)
    )

    espacios_ocupados = defaultdict(list)
    for row in filas:
        espacios_ocupados[row.id_espacio].append({
            'hora_inicio': row.hora_inicio
        })

    print(f"\nEspacios ocupados el {fecha}:")
    for espacio_id, horarios in espacios_ocupados.items():
        print(f"Espacio {espacio_id}:")
        for horario in horarios:
            print(f"   - A partir de las {horario['hora_inicio']}")

    return espacios_ocupados


def listar_todos_usuarios():
    """
    Lista todos los usuarios registrados en el sistema
    """
    rows = session.execute("SELECT dpi, nombre, email, telefono FROM usuario")

    print("\n=== LISTA DE TODOS LOS USUARIOS ===")
    usuarios = []
    for i, row in enumerate(rows, 1):
        usuario_info = {
            'dpi': row.dpi,
            'nombre': row.nombre,
            'email': row.email,
            'telefono': row.telefono
        }
        usuarios.append(usuario_info)
        print(
            f"{i}. DPI: {row.dpi} | Nombre: {row.nombre} | Email: {row.email} | Tel: {row.telefono}")

    print(f"Total: {len(usuarios)} usuarios")
    return usuarios


def listar_todos_espacios():
    """
    Lista todos los espacios disponibles con sus detalles
    """
    rows = session.execute(
        "SELECT id_espacio, nombre, tipo, capacidad_maxima, ubicacion FROM espacio")

    print("\n=== LISTA DE TODOS LOS ESPACIOS ===")
    espacios = []
    for i, row in enumerate(rows, 1):
        espacio_info = {
            'id': row.id_espacio,
            'nombre': row.nombre,
            'tipo': row.tipo,
            'capacidad': row.capacidad_maxima,
            'ubicacion': row.ubicacion
        }
        espacios.append(espacio_info)
        print(f"{i}. ID: {row.id_espacio} | Nombre: {row.nombre} | Tipo: {row.tipo} | Capacidad: {row.capacidad_maxima} | Ubicación: {row.ubicacion}")

    print(f"Total: {len(espacios)} espacios")
    return espacios


def reservas_por_espacio(espacio_id, fecha_inicio=None, fecha_fin=None):
    """
    Muestra todas las reservas de un espacio específico, opcionalmente en un rango de fechas
    """
    if fecha_inicio and fecha_fin:
        query = session.prepare("""
            SELECT fecha, hora_inicio, hora_fin, dpi, nombre_usuario, estado 
            FROM reservas_por_espacio 
            WHERE id_espacio=? AND fecha >= ? AND fecha <= ?
        """)
        params = (espacio_id, fecha_inicio, fecha_fin)
    else:
        query = session.prepare("""
            SELECT fecha, hora_inicio, hora_fin, dpi, nombre_usuario, estado 
            FROM reservas_por_espacio 
            WHERE id_espacio=?
        """)
        params = (espacio_id,)

    rows = session.execute(query, params)

    print(f"\nReservas del espacio {espacio_id}:")
    reservas = []
    for i, row in enumerate(rows, 1):
        reserva_info = {
            'fecha': row.fecha,
            'hora_inicio': row.hora_inicio,
            'hora_fin': row.hora_fin,
            'dpi': row.dpi,
            'usuario': row.nombre_usuario,
            'estado': row.estado
        }
        reservas.append(reserva_info)
        print(f"{i}. {row.fecha} | {row.hora_inicio}-{row.hora_fin} | Usuario: {row.nombre_usuario} ({row.dpi}) | Estado: {row.estado}")

    print(f"Total reservas: {len(reservas)}")
    return reservas


def estadisticas_uso_espacios(fecha_inicio, fecha_fin):
    """
    Muestra estadísticas de uso de espacios en un rango de fechas
    """
    espacios = get_espacios()
    estadisticas = {}

    for espacio_id in espacios:
        query = session.prepare("""
            SELECT COUNT(*) as total 
            FROM reservas_por_espacio 
            WHERE id_espacio=? AND fecha >= ? AND fecha <= ?
        """)
        result = session.execute(query, (espacio_id, fecha_inicio, fecha_fin))
        total_reservas = result.one().total

        estadisticas[espacio_id] = total_reservas

    print(f"\n=== ESTADÍSTICAS DE USO ({fecha_inicio} a {fecha_fin}) ===")
    for espacio_id, total in sorted(estadisticas.items(), key=lambda x: x[1], reverse=True):
        print(f"Espacio {espacio_id}: {total} reservas")

    return estadisticas


def usuarios_mas_activos(fecha_inicio, fecha_fin):
    """
    Muestra los usuarios con más reservas en un período
    """
    # Primero obtenemos todos los usuarios
    usuarios_rows = session.execute("SELECT dpi, nombre FROM usuario")
    usuarios_stats = {}

    for row in usuarios_rows:
        query = session.prepare("""
            SELECT COUNT(*) as total 
            FROM reservas_por_usuario 
            WHERE dpi=? AND fecha >= ? AND fecha <= ?
        """)
        result = session.execute(query, (row.dpi, fecha_inicio, fecha_fin))
        total_reservas = result.one().total

        if total_reservas > 0:
            usuarios_stats[row.dpi] = {
                'nombre': row.nombre,
                'reservas': total_reservas
            }

    print(f"\n=== USUARIOS MÁS ACTIVOS ({fecha_inicio} a {fecha_fin}) ===")
    for i, (dpi, info) in enumerate(sorted(usuarios_stats.items(), key=lambda x: x[1]['reservas'], reverse=True), 1):
        print(f"{i}. {info['nombre']} ({dpi}): {info['reservas']} reservas")

    return usuarios_stats


# Verificar que los datos se cargaron correctamente
verificar_datos()

# listar_todos_usuarios()
listar_todos_espacios()

# esapcios_disponibles(date(2025, 5, 1))
espacios_ocupados(date(2025, 5, 1))


# ====== Reservas de usaurio por rango de fechas ======
# historial_reservas_usuario("DPI001", date(2025, 4, 1), date(2025, 1, 10))

# ====== Reservas de un espacio en rango de fechas ======
# ocupacion_por_fechas(date(2025, 4, 1), date(2025, 4, 3))
