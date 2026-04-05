
import sys
import os
from pathlib import Path

# Configurar rutas
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from utils.config import MONGO_DB, POSTGRES_URI, COLLECTION_RAW_MATCHES
from utils.db import get_mongo_client
import psycopg2
from datetime import datetime

def format_duration(seconds):
    if not seconds: return "???"
    return f"{int(seconds // 60)}m {int(seconds % 60)}s"

def inspect_mongo():
    print("\n" + "🍃" * 15)
    print("   MONGODB (Muestra de Partidas L0/L1)")
    print("🍃" * 15)
    
    with get_mongo_client() as client:
        db = client[MONGO_DB]
        collections = sorted(db.list_collection_names())
        
        for col_name in collections:
            if col_name.startswith("L0_") or col_name.startswith("L1_") or col_name == COLLECTION_RAW_MATCHES:
                count = db[col_name].count_documents({})
                print(f"\n📦 {col_name} ({count} docs)")
                
                # Obtener muestra de las últimas 5
                cursor = db[col_name].find({}, {"_id": 1, "metadata": 1, "info": 1, "data": 1}).sort("_id", -1).limit(5)
                for doc in cursor:
                    mid = doc.get("_id")
                    if "metadata" in doc and "matchId" in doc["metadata"]:
                        mid = doc["metadata"]["matchId"]
                    elif "data" in doc and "metadata" in doc["data"]:
                        mid = doc["data"]["metadata"].get("matchId", mid)
                    
                    print(f"   - Match ID: {mid}")

def inspect_postgres():
    print("\n" + "🐘" * 15)
    print("   POSTGRESQL (Muestra de Partidas)")
    print("🐘" * 15)
    
    pg_dsn = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")
    try:
        conn = psycopg2.connect(pg_dsn)
        cur = conn.cursor()
        
        # 1. Ver tablas principales
        print(f"--- TABLA: matches ---")
        cur.execute("SELECT match_id, pool_id, game_start_at, duration_s FROM matches ORDER BY game_start_at DESC LIMIT 10")
        rows = cur.fetchall()
        if not rows:
            print(" ❌ Tabla 'matches' vacía.")
        for mid, pid, start, dur in rows:
            start_str = start.strftime("%Y-%m-%d %H:%M:%S") if start else "???"
            print(f"   - [{pid[:10]:<10}] {mid} | {start_str} | {format_duration(dur)}")

        conn.close()
    except Exception as e:
        print(f" ❌ Error al conectar con Postgres: {e}")

def main():
    print("=" * 70)
    print("🔍 INSPECCIÓN DE ARQUITECTURA REAL (Mongo Colecciones & Postgres Tablas)")
    print("=" * 70)
    
    inspect_mongo()
    inspect_postgres()
    
    print("\n" + "=" * 70)

if __name__ == "__main__":
    main()
