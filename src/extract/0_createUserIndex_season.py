# L0_build_users_index_season.py
# Construye un índice unificado de usuarios para el modo SEASON desde:
# - mapa_cuentas_season.json (source of truth para season)
# Los PUUID se obtienen SIEMPRE desde la API de Riot

import sys
from pathlib import Path

# —————————————————————————————
# Asegura que 'src' esté en sys.path
# —————————————————————————————
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parents[2]        # lol_data/
SRC_DIR = BASE_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# —————————————————————————————
# Ahora sí los imports reales
# —————————————————————————————
import os
import json
import datetime

from dotenv import load_dotenv
from pymongo import MongoClient
from riotwatcher import RiotWatcher

from utils.api_key_manager import get_api_key


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
REGIONAL = os.getenv("REGIONAL_ROUTING", "europe")
API_KEY = get_api_key(REGIONAL)

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# Colección específica para season
USERS_INDEX_COLL = db["L0_users_index_season"]

# Mapa de cuentas específico para season
MAP_PATH = BASE_DIR / "mapa_cuentas_season.json"

riot = RiotWatcher(API_KEY)


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def load_map(path):
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_puuid_from_api(riot_id):
    """Obtiene PUUID SIEMPRE desde Riot API."""
    try:
        name, tag = riot_id.split("#", 1)
        acc = riot.account.by_riot_id(REGIONAL, name, tag)
        return acc["puuid"]
    except Exception:
        return None


def main():
    print("[BOOT] Iniciando L0_build_users_index_season.py")

    # Load mapa_cuentas_season.json
    mapa = load_map(MAP_PATH)
    if not mapa:
        print(f"[ERROR] mapa_cuentas_season.json not found or empty at {MAP_PATH}")
        return
    
    print(f"[INFO] mapa_cuentas_season.json contiene {len(mapa)} personas")

    # NO hacer drop - actualizar/añadir documentos existentes
    # Esto permite que la pool "season" se mantenga estable
    print("[INFO] Actualizando L0_users_index_season (sin borrar existentes)")

    now = now_utc()

    # Process all personas from mapa_cuentas_season.json
    updated = 0
    inserted = 0
    
    for persona, cuentas in mapa.items():
        if not isinstance(cuentas, list):
            print(f"[WARN] Skipping {persona}: accounts is not a list")
            continue
            
        riot_ids = []
        puuids = []
        accounts = []

        for rid in cuentas:
            puuid = get_puuid_from_api(rid)
            if puuid:
                riot_ids.append(rid)
                puuids.append(puuid)
                accounts.append({"riotId": rid, "puuid": puuid})
            else:
                print(f"[WARN] No se pudo obtener PUUID para {rid}")

        if riot_ids:
            riot_ids = sorted(set(riot_ids))
            puuids = sorted(set(puuids))

            doc = {
                "_id": persona,
                "persona": persona,
                "riotIds": riot_ids,
                "puuids": puuids,
                "accounts": accounts,
                "updated_at": now,
            }
            
            # Upsert: actualiza si existe, inserta si no
            result = USERS_INDEX_COLL.update_one(
                {"_id": persona},
                {"$set": doc, "$setOnInsert": {"created_at": now}},
                upsert=True
            )
            
            if result.upserted_id:
                inserted += 1
            elif result.modified_count > 0:
                updated += 1

    print(f"[DONE] Season index: {inserted} insertados, {updated} actualizados")
    print(f"[DONE] Total documentos en L0_users_index_season: {USERS_INDEX_COLL.count_documents({})}")


if __name__ == "__main__":
    main()
