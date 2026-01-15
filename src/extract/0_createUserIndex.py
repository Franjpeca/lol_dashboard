# L0_build_users_index.py
# Construye un índice unificado de usuarios reales desde:
# - mapa_cuentas.json (único source of truth)
# Los PUUID se obtienen SIEMPRE desde la API de Riot (nunca de riot_accounts)

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

USERS_INDEX_COLL = db["L0_users_index"]

MAP_PATH = Path("mapa_cuentas.json")

riot = RiotWatcher(API_KEY)


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def parse_player(line):
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    if "#" in s:
        a, b = s.split("#", 1)
        return f"{a.strip()}#{b.strip()}"
    if "," in s:
        a, b = s.split(",", 1)
        return f"{a.strip()}#{b.strip()}"
    return None


def load_players(path):
    players = []
    if not path.exists():
        return players
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            p = parse_player(line)
            if p:
                players.append(p)
    return players


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
    print("[BOOT] Iniciando L0_build_users_index.py")

    # Load mapa_cuentas.json (single source of truth)
    mapa = load_map(MAP_PATH)
    if not mapa:
        print(f"[ERROR] mapa_cuentas.json not found or empty at {MAP_PATH}")
        return
    
    print(f"[INFO] mapa_cuentas.json contiene {len(mapa)} personas")

    USERS_INDEX_COLL.drop()
    print("[INFO] L0_users_index reiniciada")

    docs = []
    now = now_utc()

    # Process all personas from mapa_cuentas.json
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

            docs.append({
                "_id": persona,
                "persona": persona,
                "riotIds": riot_ids,
                "puuids": puuids,
                "accounts": accounts,
                "created_at": now,
                "updated_at": now,
            })

    # Insert final
    if docs:
        USERS_INDEX_COLL.insert_many(docs)
        print(f"[DONE] Insertados {len(docs)} usuarios en L0_users_index")
    else:
        print("[WARN] No se insertaron documentos")

    print("[DONE] L0_build_users_index.py finalizado")


if __name__ == "__main__":
    main()
