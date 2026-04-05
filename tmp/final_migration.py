
import os
import psycopg2
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

# Config
OLD_ID = "ca879f16"
NEW_ID = "villaquesitos"

# Postgres connection
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")
PG_DB = os.getenv("POSTGRES_DB")
PG_HOSTS = ["127.0.0.1", "localhost", "postgres"] # Probar varios hosts comunes
PG_PORT = os.getenv("POSTGRES_PORT", "5432")

def get_pg_conn():
    for host in PG_HOSTS:
        try:
            conn = psycopg2.connect(
                user=PG_USER,
                password=PG_PASS,
                database=PG_DB,
                host=host,
                port=PG_PORT,
                connect_timeout=3
            )
            print(f"Connected to Postgres via {host}")
            return conn
        except:
            continue
    return None

def migrate_postgres():
    conn = get_pg_conn()
    if not conn:
        print("FAILED to connect to Postgres. Manual intervention required.")
        return
    
    try:
        with conn.cursor() as cur:
            # Update pools
            cur.execute("UPDATE pools SET pool_id = %s WHERE pool_id = %s", (NEW_ID, OLD_ID))
            print(f"Postgres: Updated pools: {cur.rowcount} rows.")
            
            # Update matches
            cur.execute("UPDATE matches SET pool_id = %s WHERE pool_id = %s", (NEW_ID, OLD_ID))
            print(f"Postgres: Updated matches: {cur.rowcount} rows.")
            
            # Update player_performances
            cur.execute("UPDATE player_performances SET pool_id = %s WHERE pool_id = %s", (NEW_ID, OLD_ID))
            print(f"Postgres: Updated player_performances: {cur.rowcount} rows.")

            # Metric tables (all of them)
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'metric_%'")
            metrics_tables = [r[0] for r in cur.fetchall()]
            for table in metrics_tables:
                cur.execute(f"UPDATE {table} SET pool_id = %s WHERE pool_id = %s", (NEW_ID, OLD_ID))
                if cur.rowcount > 0:
                    print(f"Postgres: Updated {table}: {cur.rowcount} rows.")
            
            conn.commit()
            print("Postgres migration finished successfully.")
    except Exception as e:
        print(f"Postgres ERROR: {e}")
    finally:
        conn.close()

def migrate_mongo():
    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_DB_NAME = os.getenv("MONGO_DB", "lol_data")
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        colls = db.list_collection_names()
        for c in colls:
            if OLD_ID in c:
                new_c = c.replace(OLD_ID, NEW_ID)
                db[c].rename(new_c)
                print(f"Mongo: Renamed {c} -> {new_c}")
        client.close()
        print("Mongo migration finished successfully.")
    except Exception as e:
        print(f"Mongo ERROR: {e}")

if __name__ == "__main__":
    migrate_postgres()
    migrate_mongo()
