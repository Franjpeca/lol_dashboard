
import sys
import os
from pathlib import Path

# Configurar rutas
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from utils.config import MONGO_DB, POSTGRES_URI, COLLECTION_RAW_MATCHES
from utils.db import get_mongo_client
import psycopg2

def inspect_mongo():
    print("\n" + "🍃" * 15)
    print("   MONGODB (Datos Crudos - L0)")
    print("🍃" * 15)
    
    with get_mongo_client() as client:
        db = client[MONGO_DB]
        collections = sorted(db.list_collection_names())
        
        found = False
        for col_name in collections:
            # Solo mostramos colecciones de partidas o índices (L0)
            if col_name.startswith("L0_") or col_name == COLLECTION_RAW_MATCHES:
                count = db[col_name].count_documents({})
                print(f" - {col_name:<40} : {count:>6} documentos")
                found = True
        
        if not found:
            print(" ❌ No se encontraron colecciones L0.")

def inspect_postgres():
    print("\n" + "🐘" * 15)
    print("   POSTGRESQL (Datos Filtrados - L1)")
    print("🐘" * 15)
    
    pg_dsn = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")
    try:
        conn = psycopg2.connect(pg_dsn)
        cur = conn.cursor()
        
        # Consultar todas las tablas que empiecen por L1 (partidas filtradas)
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
              AND table_name LIKE 'l1_%%'
            ORDER BY table_name
        """
        cur.execute(query)
        tables = [row[0] for row in cur.fetchall()]
        
        if not tables:
            print(" ❌ No se encontraron tablas L1.")
        else:
            for table in tables:
                # Contamos filas de cada tabla
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f" - {table:<40} : {count:>6} filas")
        
        conn.close()
    except Exception as e:
        print(f" ❌ Error al conectar con Postgres: {e}")

def main():
    print("=" * 60)
    print("🔍 INSPECCIÓN TOTAL DE PARTIDAS (L0 -> LX)")
    print("=" * 60)
    
    inspect_mongo()
    inspect_postgres()
    
    print("\n" + "=" * 60)
    print("✅ Inspección finalizada.")

if __name__ == "__main__":
    main()
