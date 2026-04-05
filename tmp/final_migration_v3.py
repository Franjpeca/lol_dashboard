
import os, psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
NEW_ID = "villaquesitos"
PG_URI = os.getenv("POSTGRES_URI").replace("postgresql+psycopg2://", "postgresql://")

def migrate():
    try:
        conn = psycopg2.connect(PG_URI)
        with conn.cursor() as cur:
            # 1. Encontrar todos los IDs de pool "basura" o antiguos
            # Buscamos en matches y player_performances porque 'pools' puede estar vacío o incompleto
            all_ids = set()
            cur.execute("SELECT DISTINCT pool_id FROM player_performances WHERE pool_id NOT IN ('season', %s)", (NEW_ID,))
            for r in cur.fetchall(): all_ids.add(r[0])
            cur.execute("SELECT DISTINCT pool_id FROM matches WHERE pool_id NOT IN ('season', %s)", (NEW_ID,))
            for r in cur.fetchall(): all_ids.add(r[0])
            
            if not all_ids:
                print("No quedan pools antiguos que migrar.")
            
            for old_id in all_ids:
                print(f"Fusionando {old_id} -> {NEW_ID}...")
                
                # Borramos los conflictos en matches (si el mismo match_id ya existe en villaquesitos, borramos el de villaquesitos para que entre el viejo con su historico)
                cur.execute("DELETE FROM matches WHERE pool_id = %s AND match_id IN (SELECT match_id FROM matches WHERE pool_id = %s)", (NEW_ID, old_id))
                cur.execute("DELETE FROM player_performances WHERE pool_id = %s AND match_id IN (SELECT match_id FROM matches WHERE pool_id = %s)", (NEW_ID, old_id))
                
                # Actualizar
                cur.execute("UPDATE matches SET pool_id = %s WHERE pool_id = %s", (NEW_ID, old_id))
                print(f" - Tabla matches: {cur.rowcount} filas movidas.")
                
                cur.execute("UPDATE player_performances SET pool_id = %s WHERE pool_id = %s", (NEW_ID, old_id))
                print(f" - Tabla player_performances: {cur.rowcount} filas movidas.")
                
                # Borrar de la tabla maestra de pools para evitar el error de PKEY
                cur.execute("DELETE FROM pools WHERE pool_id = %s", (old_id,))
                print(f" - Tabla pools: registro {old_id} eliminado.")
            
            # Asegurar que villaquesitos tiene su fila en 'pools' para el selector del dashboard
            # Usamos valores por defecto comunes
            for mf in [1, 2, 3, 4, 5]:
                cur.execute("""
                    INSERT INTO pools (pool_id, min_friends, queue_id, personas) 
                    VALUES (%s, %s, 440, '{}') 
                    ON CONFLICT (pool_id, min_friends) DO NOTHING
                """, (NEW_ID, mf))
            
        conn.commit()
        print("✅ Postgres: Fusión completada con éxito.")
        
        # MongoDB: Renombrar colecciones L1
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("MONGO_DB", "lol_data")]
        colls = db.list_collection_names()
        for coll in colls:
            if "pool_" in coll and NEW_ID not in coll and "season" not in coll:
                # Extraer el hash (todo lo que viene despues de pool_)
                parts = coll.split("pool_")
                prefix = parts[0] + "pool_"
                old_hash = parts[1]
                new_n = prefix + NEW_ID
                
                if new_n in colls:
                    print(f"Mongo: Eliminando coleccion destino duplicada {new_n}")
                    db[new_n].drop()
                
                db[coll].rename(new_n)
                print(f"Mongo: {coll} -> {new_n}")
                
        print("✅ MongoDB: Colecciones renombradas.")
        
    except Exception as e:
        print(f"❌ Error en la migración v3: {e}")
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    migrate()
