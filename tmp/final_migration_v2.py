
import os, psycopg2, psycopg2.extras
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
NEW_ID = "villaquesitos"
# El ID que tenías antes. Si hay más, el script los buscará.
OLD_IDS = ["ca879f16"] 

PG_URI = os.getenv("POSTGRES_URI").replace("postgresql+psycopg2://", "postgresql://")

def migrate():
    try:
        conn = psycopg2.connect(PG_URI)
        with conn.cursor() as cur:
            # 1. Identificar pools antiguos de TODAS las tablas principales
            found_old_ids = set()
            for table in ['pools', 'matches', 'player_performances']:
                cur.execute(f"SELECT DISTINCT pool_id FROM {table} WHERE pool_id NOT IN ('season', %s)", (NEW_ID,))
                for r in cur.fetchall():
                    found_old_ids.add(r[0])
            
            if not found_old_ids:
                print("No se encontraron pools antiguos para migrar (o ya están como 'villaquesitos').")
            
            for old_id in found_old_ids:
                print(f"Migrando {old_id} -> {NEW_ID}...")
                # Actualizar tablas reales
                # NOTA: No incluimos las vistas (metric_...) porque se actualizan solas
                for table in ['pools', 'matches', 'player_performances']:
                    cur.execute(f"UPDATE {table} SET pool_id = %s WHERE pool_id = %s", (NEW_ID, old_id))
                    print(f" - Tabla {table}: {cur.rowcount} filas movidas.")
            
            # Asegurar que existe el registro en la tabla 'pools' para Villaquesitos
            cur.execute("SELECT 1 FROM pools WHERE pool_id = %s", (NEW_ID,))
            if not cur.fetchone():
                print(f"Creando registro maestro para pool '{NEW_ID}'...")
                # Intentamos copiar la config de 'season' o una por defecto
                cur.execute("""
                    INSERT INTO pools (pool_id, min_friends, queue_id, personas) 
                    VALUES (%s, 5, 440, '{}') 
                    ON CONFLICT DO NOTHING
                """, (NEW_ID,))
            
        conn.commit()
        print("✅ Migración de Postgres completada.")
        
        # 2. MongoDB
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("MONGO_DB", "lol_data")]
        colls = db.list_collection_names()
        renamed_count = 0
        for coll in colls:
            # Si la colección contiene un hash de 8 caracteres al final o el OLD_ID
            # Ej: L1_q440_min5_pool_ca879f16
            if "pool_" in coll and NEW_ID not in coll and "season" not in coll:
                old_hash = coll.split("pool_")[-1]
                new_name = coll.replace(old_hash, NEW_ID)
                if new_name not in colls:
                    db[coll].rename(new_name)
                    print(f"Mongo: {coll} -> {new_name}")
                    renamed_count += 1
        print(f"✅ Migración de MongoDB completada ({renamed_count} colecciones).")
    except Exception as e:
        print(f"❌ Error en la migración: {e}")

if __name__ == "__main__":
    migrate()
