from __future__ import annotations

from mysql.connector.connection import MySQLConnection

from .db import execute, fetch_all


def listar_incidencias_activas(conn: MySQLConnection) -> list[dict]:
    """
    Devuelve incidencias cuyo estado NO sea 'cerrada'.

    Requisitos:
    - Debe devolver una lista de diccionarios (una fila por dict).
    - Debe ordenar primero por prioridad (alta > media > baja) y luego por fecha_apertura ascendente.
    - Debe usar fetch_all(conn, sql, params) para ejecutar el SELECT.

    Pista: se puede ordenar por prioridad usando CASE en SQL.
    """
    raise NotImplementedError


def listar_incidencias_sin_tecnico(conn: MySQLConnection) -> list[dict]:
    """
    Devuelve incidencias activas sin técnico asignado.

    Requisitos:
    - tecnico_id IS NULL
    - estado <> 'cerrada'
    - ordenar por fecha_apertura ascendente
    """
    raise NotImplementedError


def crear_incidencia(conn: MySQLConnection, equipo_id: int, descripcion: str, prioridad: str = "media") -> int:
    """
    Crea una incidencia nueva en estado 'abierta'.

    Validaciones:
    - descripcion no puede ser vacía ni solo espacios (ValueError)
    - prioridad debe ser 'baja', 'media' o 'alta' (ValueError)
    - equipo_id debe ser entero positivo (ValueError)

    Requisitos SQL:
    - INSERT en tabla incidencias
    - tecnico_id debe ser NULL
    - estado debe ser 'abierta'
    - fecha_apertura debe ser NOW()
    - fecha_cierre debe ser NULL

    Debe devolver el número de filas afectadas (normalmente 1).
    """
    raise NotImplementedError


def asignar_tecnico(conn: MySQLConnection, incidencia_id: int, tecnico_id: int) -> int:
    """
    Asigna un técnico a una incidencia y la marca como 'en_proceso' si la incidencia no está cerrada.

    Validaciones:
    - incidencia_id y tecnico_id deben ser enteros positivos (ValueError)

    Requisitos:
    - UPDATE sobre incidencias
    - Solo debe actualizar si estado <> 'cerrada'
    - Debe devolver filas afectadas (0 si no existe o ya está cerrada)
    """
    raise NotImplementedError


def cerrar_incidencia(conn: MySQLConnection, incidencia_id: int) -> int:
    """
    Cierra una incidencia.

    Validaciones:
    - incidencia_id debe ser entero positivo (ValueError)

    Requisitos:
    - UPDATE sobre incidencias
    - estado='cerrada'
    - fecha_cierre=NOW()
    - Solo debe cerrar si estado <> 'cerrada'
    - Devuelve filas afectadas
    """
    raise NotImplementedError


def detalle_incidencias_join(conn: MySQLConnection) -> list[dict]:
    """
    Devuelve una vista enriquecida con datos de equipo y técnico.

    Columnas mínimas esperadas:
    - i.id, i.descripcion, i.prioridad, i.estado, i.fecha_apertura, i.fecha_cierre
    - e.tipo, e.modelo, e.ubicacion, e.estado AS estado_equipo
    - t.nombre AS tecnico (puede ser NULL si no hay técnico)

    Requisitos:
    - JOIN equipos (obligatorio)
    - LEFT JOIN tecnicos (opcional)
    - Ordenar por estado, prioridad DESC, fecha_apertura ASC
    """
    raise NotImplementedError
