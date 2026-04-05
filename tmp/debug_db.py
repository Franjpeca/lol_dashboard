import psycopg2
from pathlib import Path
import sys

# Configuración básica para conectar
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from utils.config import POSTGRES_URI

_PG_DSN = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")

def debug_db():
    print("--- 🔍 DEBUG DE TABLAS ---")
    try:
        conn = psycopg2.connect(_PG_DSN)
        cur = conn.cursor()
        
        print("\n1. Contenido de la tabla 'pools':")
        cur.execute("SELECT pool_id, min_friends, queue_id FROM pools")
        rows = cur.fetchall()
        for r in rows:
            print(f"   - ID: {r[0]} | Min: {r[1]} | Queue: {r[2]}")

        print("\n2. Conteo de registros en 'player_performances' por pool_id:")
        cur.execute("SELECT pool_id, COUNT(*) FROM player_performances GROUP BY pool_id")
        rows = cur.fetchall()
        for r in rows:
            print(f"   - Pool: {r[0]} | Registros: {r[1]}")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    debug_db()
