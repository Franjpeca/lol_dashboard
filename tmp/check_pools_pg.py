
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

PG_URI = os.getenv("POSTGRES_URI").replace("postgresql+psycopg2://", "postgresql://")

try:
    conn = psycopg2.connect(PG_URI)
    with conn.cursor() as cur:
        cur.execute("SELECT pool_id, min_friends, queue_id FROM pools ORDER BY pool_id, min_friends")
        rows = cur.fetchall()
        print("--- POOLS TABLE ---")
        for r in rows:
            print(r)
    conn.close()
except Exception as e:
    print(f"Error: {e}")
