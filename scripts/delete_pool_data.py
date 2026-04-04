
import sys
import argparse
import psycopg2
from pathlib import Path

# Asegurar que src/ esté en sys.path para importar utils
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parents[1]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utils.config import MONGO_DB, POSTGRES_URI
from utils.db import get_mongo_client

# psycopg2 necesita DSN sin el prefijo SQLAlchemy si se usa
_PG_DSN = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")

def main():
    parser = argparse.ArgumentParser(description="Borra datos de una pool en Mongo y Postgres")
    parser.add_argument("pool_id", help="ID de la pool a borrar (ej: ca879f16 o season)")
    parser.add_argument("--force", action="store_true", help="Borrar sin confirmar")
    args = parser.parse_args()

    pool_id = args.pool_id
    print(f"\n⚠️  ATENCIÓN: Vas a borrar TODOS los datos procesados de la pool: {pool_id}")
    print("Esto afectará a PostgreSQL (matches, performances) y MongoDB (colecciones L1).")
    
    if not args.force:
        confirm = input("¿Confirmas el borrado? (escribe 'yes' para continuar): ")
        if confirm.lower() != 'yes':
            print("Operación cancelada.")
            return

    # 1. PostgreSQL
    print(f"\n[PG] Conectando a PostgreSQL...")
    try:
        conn = psycopg2.connect(_PG_DSN)
        with conn.cursor() as cur:
            # Borrar performances
            cur.execute("DELETE FROM player_performances WHERE pool_id = %s", (pool_id,))
            p_count = cur.rowcount
            print(f"  - Player performances eliminados: {p_count}")
            
            # Borrar matches
            cur.execute("DELETE FROM matches WHERE pool_id = %s", (pool_id,))
            m_count = cur.rowcount
            print(f"  - Matches eliminados: {m_count}")
            
            # Borrar de la tabla pools
            cur.execute("DELETE FROM pools WHERE pool_id = %s", (pool_id,))
            pool_deleted = cur.rowcount
            print(f"  - Registro en tabla 'pools' eliminado: {pool_deleted}")
            
        conn.commit()
        conn.close()
        print("[PG] ✅ Datos eliminados correctamente.")
    except Exception as e:
        print(f"[PG] ❌ Error: {e}")

    # 2. MongoDB
    print(f"\n[MONGO] Conectando a MongoDB (DB: {MONGO_DB})...")
    try:
        with get_mongo_client() as client:
            db = client[MONGO_DB]
            all_colls = db.list_collection_names()
            
            # 2a. Borrar colecciones L1 (datos procesados)
            targets = [c for c in all_colls if f"pool_{pool_id}" in c or c.endswith(f"_{pool_id}")]
            for t in targets:
                db[t].drop()
                print(f"  - Colección L1 eliminada: {t}")

            # 2b. Si es la pool 'season', borrar también su índice de usuarios L0
            if pool_id == "season" and "L0_users_index_season" in all_colls:
                db["L0_users_index_season"].drop()
                print("  - Colección raíz 'L0_users_index_season' eliminada.")

        print("[MONGO] ✅ Colecciones eliminadas correctamente.")
    except Exception as e:
        print(f"[MONGO] ❌ Error: {e}")

    print("\n[DONE] Limpieza completa: La pool ya no aparecerá en el Dashboard.")

if __name__ == "__main__":
    main()
