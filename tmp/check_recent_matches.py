
import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient
import psycopg2
from riotwatcher import LolWatcher

# Configuración de rutas
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parents[1]
sys.path.insert(0, str(BASE_DIR / "src"))

load_dotenv(dotenv_path=BASE_DIR / ".env")

from utils.api_key_manager import get_api_key
from utils.config import MONGO_URI, MONGO_DB, POSTGRES_URI, REGIONAL_ROUTING

def get_puuid(riot_id):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    # Buscar en L0_users_index
    doc = db["L0_users_index"].find_one({"accounts.riotId": riot_id})
    if not doc:
        return None
    for acc in doc["accounts"]:
        if acc["riotId"] == riot_id:
            return acc["puuid"]
    return None

def check_db(puuid):
    print(f"\n--- [DB] Últimas 5 partidas Flex en Postgres (Pool: villaquesitos) ---")
    pg_uri = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")
    try:
        conn = psycopg2.connect(pg_uri)
        cur = conn.cursor()
        query = """
            SELECT m.match_id, m.game_start_at 
            FROM matches m
            JOIN player_performances pp ON m.match_id = pp.match_id AND m.pool_id = pp.pool_id
            WHERE pp.puuid = %s AND m.pool_id = 'villaquesitos' AND m.queue_id = 440
            ORDER BY m.game_start_at DESC
            LIMIT 5
        """
        cur.execute(query, (puuid,))
        rows = cur.fetchall()
        for r in rows:
            print(f"ID: {r[0]} | Fecha: {r[1]}")
        if not rows: print("No se encontraron partidas.")
        conn.close()
    except Exception as e:
        print(f"Error Postgres: {e}")

def check_api(puuid):
    print(f"\n--- [API] Últimas 5 partidas Flex directamente de Riot ---")
    api_key = get_api_key(REGIONAL_ROUTING)
    watcher = LolWatcher(api_key)
    try:
        match_ids = watcher.match.matchlist_by_puuid(REGIONAL_ROUTING, puuid, count=5, queue=440)
        for mid in match_ids:
            m_data = watcher.match.by_id(REGIONAL_ROUTING, mid)
            ts = m_data["info"]["gameStartTimestamp"] / 1000
            dt = datetime.fromtimestamp(ts)
            print(f"ID: {mid} | Fecha: {dt}")
        if not match_ids: print("No se encontraron partidas en la API (cola 440).")
    except Exception as e:
        print(f"Error Riot API: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python tmp/check_recent_matches.py \"Nombre#TAG\"")
        sys.exit(1)
        
    riot_id = sys.argv[1]
    print(f"Investigando cuenta: {riot_id}")
    puuid = get_puuid(riot_id)
    if not puuid:
        print(f"No se encontró el PUUID para {riot_id} en MongoDB (L0_users_index).")
    else:
        print(f"PUUID: {puuid}")
        check_db(puuid)
        check_api(puuid)
