
import os
import psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# Config
OLD_POOL_ID = "ca879f16"
NEW_POOL_ID = "villaquesitos"
POSTGRES_URI = os.getenv("POSTGRES_URI").replace("postgresql+psycopg2://", "postgresql://")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "lol_data")

def migrate_postgres():
    print(f"--- Migrating Postgres: {OLD_POOL_ID} -> {NEW_POOL_ID} ---")
    try:
        conn = psycopg2.connect(POSTGRES_URI)
        with conn.cursor() as cur:
            # Update pools table
            cur.execute("UPDATE pools SET pool_id = %s WHERE pool_id = %s", (NEW_POOL_ID, OLD_POOL_ID))
            print(f"Updated pools table: {cur.rowcount} rows affected.")
            
            # Update matches table
            cur.execute("UPDATE matches SET pool_id = %s WHERE pool_id = %s", (NEW_POOL_ID, OLD_POOL_ID))
            print(f"Updated matches table: {cur.rowcount} rows affected.")
            
        conn.commit()
        conn.close()
        print("Postgres migration: SUCCESS")
    except Exception as e:
        print(f"Postgres migration: ERROR: {e}")

def migrate_mongo():
    print(f"--- Migrating MongoDB Collections: {OLD_POOL_ID} -> {NEW_POOL_ID} ---")
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        
        # Rename collections like L1_q440_min5_pool_ca879f16 to L1_q440_min5_pool_villaquesitos
        colls = db.list_collection_names()
        renamed = 0
        for name in colls:
            if OLD_POOL_ID in name:
                new_name = name.replace(OLD_POOL_ID, NEW_POOL_ID)
                db[name].rename(new_name)
                print(f"Renamed Mongo collection: {name} -> {new_name}")
                renamed += 1
        
        client.close()
        print(f"Mongo migration: SUCCESS ({renamed} collections renamed)")
    except Exception as e:
        print(f"Mongo migration: ERROR: {e}")

if __name__ == "__main__":
    migrate_postgres()
    migrate_mongo()
