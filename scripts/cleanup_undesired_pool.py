import os
import sys
import shutil
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

def main():
    # Caminos
    ROOT = Path(__file__).resolve().parents[1]
    env_path = ROOT / ".env"
    
    # Carga de entorno con expansiones
    load_dotenv(dotenv_path=env_path)
    
    # ID de la pool a eliminar
    POOL_ID = "87f3d134"
    print(f"--- INICIANDO LIMPIEZA DE POOL: {POOL_ID} ---")
    
    # 1. MongoDB
    try:
        mongo_uri = os.getenv("MONGO_URI")
        db_name = os.getenv("MONGO_DB", "lol_data")
        
        if not mongo_uri:
            print("[WARN] MONGO_URI no encontrado.")
        else:
            client = MongoClient(mongo_uri)
            db = client[db_name]
            
            # Listar colecciones
            all_colls = db.list_collection_names()
            to_drop = [c for c in all_colls if POOL_ID in c]
            
            if not to_drop:
                print(f"[MONGO] No se encontraron colecciones para la pool {POOL_ID}")
            else:
                for coll in to_drop:
                    print(f"[MONGO] Eliminando colección: {coll}")
                    db[coll].drop()
                print(f"[MONGO] Finalizado. {len(to_drop)} colecciones eliminadas.")
            client.close()
    except Exception as e:
        print(f"[ERROR MONGO] No se pudo limpiar la base de datos: {e}")

    # 2. Filesystem (data/results)
    results_dir = ROOT / "data" / "results" / f"pool_{POOL_ID}"
    if results_dir.exists():
        print(f"[FS] Eliminando resultados: {results_dir}")
        shutil.rmtree(results_dir)
    else:
        print(f"[FS] No se encontró carpeta de resultados.")

    # 3. Filesystem (data/runtime)
    runtime_dir = ROOT / "data" / "runtime" / f"pool_{POOL_ID}"
    if runtime_dir.exists():
        print(f"[FS] Eliminando runtime: {runtime_dir}")
        shutil.rmtree(runtime_dir)
    else:
        print(f"[FS] No se encontró carpeta de runtime.")

    print("\n--- LIMPIEZA FINALIZADA ---")

if __name__ == "__main__":
    main()
