"""
utils/pg.py
Context manager y helpers para conexiones PostgreSQL via psycopg2.
"""

import sys
from contextlib import contextmanager
from pathlib import Path

if str(Path(__file__).resolve().parents[1]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import psycopg2
import psycopg2.extras
from utils.config import POSTGRES_URI


@contextmanager
def get_pg_connection():
    """
    Context manager que abre y cierra una conexión PostgreSQL.

    Uso:
        with get_pg_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
            conn.commit()
    """
    conn = None
    try:
        conn = psycopg2.connect(POSTGRES_URI)
        yield conn
    except Exception:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn and not conn.closed:
            conn.close()


@contextmanager
def get_pg_cursor(row_factory=psycopg2.extras.RealDictCursor):
    """
    Context manager que abre conn + cursor y hace commit automático al salir.

    Uso:
        with get_pg_cursor() as cur:
            cur.execute("SELECT ...")
            rows = cur.fetchall()
    """
    with get_pg_connection() as conn:
        with conn.cursor(cursor_factory=row_factory) as cur:
            yield cur
        conn.commit()


def execute_many(sql: str, rows: list[dict]):
    """Inserta/actualiza múltiples filas con psycopg2.extras.execute_batch."""
    if not rows:
        return
    with get_pg_connection() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
        conn.commit()
