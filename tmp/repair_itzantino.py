import json
import psycopg2
from pathlib import Path
import sys
import os

# Configuración de rutas
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from utils.config import POSTGRES_URI, MONGO_DB
from utils.db import get_mongo_client

_PG_DSN = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")

def repair():
    print("--- 🛠️ INICIANDO REPARACIÓN DE POOL: Imperio Itzantino ---")
    
    # 1. Validar JSON
    json_path = ROOT / "data" / "mapa_cuentas_imperio_itzantino.json"
    print(f"1. Validando JSON: {json_path}")
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"   ✅ JSON válido. {len(data)} personas encontradas.")
    except Exception as e:
        print(f"   ❌ ERROR EN JSON: {e}")
        return

    # 2. Registrar en Postgres (1-5 amigos)
    print("2. Registrando en PostgreSQL...")
    try:
        conn = psycopg2.connect(_PG_DSN)
        cur = conn.cursor()
        for m in range(1, 6):
            cur.execute("""
                INSERT INTO pools (pool_id, queue_id, min_friends) 
                VALUES ('Imperio Itzantino', 0, %s) 
                ON CONFLICT DO NOTHING
            """, (m,))
        conn.commit()
        cur.close()
        conn.close()
        print("   ✅ Todas las versiones (min 1-5) registradas en Postgres.")
    except Exception as e:
        print(f"   ❌ ERROR EN POSTGRES: {e}")

    # 3. Limpiar MongoDB (por si acaso hay basura de intentos fallidos)
    print("3. Limpiando rastro en MongoDB...")
    try:
        with get_mongo_client() as client:
            db = client[MONGO_DB]
            coll_name = "L0_users_index_imperio_itzantino"
            db[coll_name].drop()
            print(f"   ✅ Colección {coll_name} reseteada para nueva ingesta.")
    except Exception as e:
        print(f"   ❌ ERROR EN MONGO: {e}")

    print("\n--- ✅ REPARACIÓN COMPLETADA ---")
    print("Ahora puedes ejecutar 'python src/run_all.py' y debería funcionar perfectamente.")

if __name__ == "__main__":
    repair()
