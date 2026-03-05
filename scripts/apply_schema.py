"""
scripts/apply_schema.py
Aplica el esquema SQL al PostgreSQL local.

Uso:
    python scripts/apply_schema.py
"""
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import psycopg2
from utils.config import POSTGRES_URI

# psycopg2 necesita DSN sin el prefijo de SQLAlchemy
def to_psycopg2_dsn(uri: str) -> str:
    return uri.replace("postgresql+psycopg2://", "postgresql://")

SQL_FILE = Path(__file__).parent / "init_db.sql"
VIEWS_FILE = Path(__file__).parent / "create_metric_views.sql"


def main():
    dsn = to_psycopg2_dsn(POSTGRES_URI)
    print(f"[SCHEMA] Conectando a PostgreSQL...")
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
    except Exception as e:
        print(f"[ERROR] No se pudo conectar: {e}")
        sys.exit(1)

    # 1. Aplicar tablas base
    sql_base = SQL_FILE.read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql_base)
    print(f"[SCHEMA] ✅ Schema aplicado correctamente desde {SQL_FILE.name}")

    # 2. Aplicar vistas de métricas
    if VIEWS_FILE.exists():
        sql_views = VIEWS_FILE.read_text(encoding="utf-8")
        with conn.cursor() as cur:
            cur.execute(sql_views)
        print(f"[SCHEMA] ✅ Vistas de métricas aplicadas desde {VIEWS_FILE.name}")
    
    conn.close()


if __name__ == "__main__":
    main()
