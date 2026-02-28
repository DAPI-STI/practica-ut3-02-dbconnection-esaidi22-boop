from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable, Optional

import mysql.connector
from mysql.connector.connection import MySQLConnection


@dataclass(frozen=True)
class DBConfig:
    """Configuración de conexión a la base de datos."""
    host: str
    port: int
    database: str
    user: str
    password: str


def load_config_from_env() -> DBConfig:
    host = os.getenv("DB_HOST", "localhost")

    port_str = os.getenv("PUERTO_DB", os.getenv("DB_PORT", "3306"))
    try:
        port = int(port_str)
    except (TypeError, ValueError):
        raise ValueError("Error en la configuración: el puerto debe ser un número entero.")

    database = os.getenv("DB_NAME", "sti_incidencias")
    user = os.getenv("DB_USER", "sti_app")
    password = os.getenv("DB_PASSWORD", "sti_app_2026")

    return DBConfig(host=host, port=port, database=database, user=user, password=password)


def get_connection(cfg: Optional[DBConfig] = None) -> MySQLConnection:
    if cfg is None:
        cfg = load_config_from_env()

    return mysql.connector.connect(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        user=cfg.user,
        password=cfg.password,
    )


def fetch_all(conn: MySQLConnection, query: str, params: Optional[Iterable[Any]] = None) -> list[dict]:
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(query, params or ())
        return cur.fetchall()
    finally:
        cur.close()


def execute(conn: MySQLConnection, query: str, params: Optional[Iterable[Any]] = None) -> int:
    cur = conn.cursor()
    try:
        cur.execute(query, params or ())
        conn.commit()
        return cur.rowcount
    finally:
        cur.close()
