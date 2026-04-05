
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
            # 1. Buscar IDs antiguos (que no sean 'season' ni 'villaquesitos')
            all_old_ids = set()
            cur.execute("SELECT DISTINCT pool_id FROM player_performances WHERE pool_id NOT IN ('season', %s)", (NEW_ID,))
            for r in cur.fetchall(): all_old_ids.add(r[0])
            cur.execute("SELECT DISTINCT pool_id FROM matches WHERE pool_id NOT IN ('season', %s)", (NEW_ID,))
            for r in cur.fetchall(): all_old_ids.add(r[0])
            cur.execute("SELECT DISTINCT pool_id FROM pools WHERE pool_id NOT IN ('season', %s)", (NEW_ID,))
            for r in cur.fetchall(): all_old_ids.add(r[0])
            
            if not all_old_ids:
                print("No se encontraron pools antiguos o huérfanos. Verificando integridad de Villaquesitos...")
            
            for old_id in all_old_ids:
                print(f"Fusionando {old_id} -> {NEW_ID}...")
                
                # Paso A: Eliminar duplicados en el destino para que el UPDATE no falle
                # Primero en Player Performances (que es la que suele dar error de Duplicate Key)
                cur.execute("""
                    DELETE FROM player_performances 
                    WHERE pool_id = %s 
                    AND match_id IN (SELECT match_id FROM player_performances WHERE pool_id = %s)
                """, (NEW_ID, old_id))
                print(f" - Limpiados conflictos en player_performances para {old_id}")

                # Segundo en Matches
                cur.execute("""
                    DELETE FROM matches 
                    WHERE pool_id = %s 
                    AND match_id IN (SELECT match_id FROM matches WHERE pool_id = %s)
                """, (NEW_ID, old_id))
                print(f" - Limpiados conflictos en matches para {old_id}")

                # Paso B: Ahora sí, actualizar los IDs antiguos al nuevo nombre nominativo
                cur.execute("UPDATE matches SET pool_id = %s WHERE pool_id = %s", (NEW_ID, old_id))
                print(f" - Tabla matches: {cur.rowcount} filas movidas.")
                
                cur.execute("UPDATE player_performances SET pool_id = %s WHERE pool_id = %s", (NEW_ID, old_id))
                print(f" - Tabla player_performances: {cur.rowcount} filas movidas.")
                
                # Borrar registro de la tabla maestra de pools para el ID viejo
                cur.execute("DELETE FROM pools WHERE pool_id = %s", (old_id,))
                print(f" - Registro de pool {old_id} eliminado de la tabla maestra.")

            # Paso C: Asegurar que Villaquesitos tiene sus entradas para min_friends del 1 al 5
            # Esto es vital para que el selector del Dashboard funcione
            for mf in [1, 2, 3, 4, 5]:
                cur.execute("""
                    INSERT INTO pools (pool_id, min_friends, queue_id, personas) 
                    VALUES (%s, %s, 440, '{}') 
                    ON CONFLICT (pool_id, min_friends) DO NOTHING
                """, (NEW_ID, mf))
            print(f"✅ Entradas de selector para '{NEW_ID}' garantizadas.")
            
        conn.commit()
        print("✅ Postgres: Consolidación completada.")
        
        # 3. MongoDB: Renombrar colecciones L1 a formato nominativo
        client = MongoClient(os.getenv("MONGO_URI"))
        db = client[os.getenv("MONGO_DB", "lol_data")]
        colls = db.list_collection_names()
        for coll in colls:
            if "pool_" in coll and NEW_ID not in coll and "season" not in coll:
                parts = coll.split("pool_")
                prefix = parts[0] + "pool_"
                old_val = parts[1]
                new_n = prefix + NEW_ID
                
                if new_n in colls:
                    db[new_n].drop()
                    print(f"Mongo: Eliminada col duplicada {new_n}")
                
                db[coll].rename(new_n)
                print(f"Mongo: {coll} -> {new_n}")
                
        print("✅ MongoDB: Consolidación completada.")
    except Exception as e:
        print(f"❌ Error crítico en limpieza v4: {e}")
        if 'conn' in locals(): conn.rollback()
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    migrate()
