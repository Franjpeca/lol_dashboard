# L0_build_users_index.py
# Construye un indice unificado de usuarios reales con:
# - players.txt (jugadores individuales)
# - mapa_cuentas.json (jugadores con varias cuentas)
# Los PUUID se obtienen SIEMPRE desde la API de Riot (nunca de riot_accounts)

import os
import json
import datetime
from pathlib import Path

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

PLAYERS_PATH = Path("players.txt")
MAP_PATH = Path("mapa_cuentas.json")

riot = RiotWatcher(API_KEY)


def now_utc():
    return datetime.datetime.now(datetime.UTC)


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

    players = load_players(PLAYERS_PATH)
    print(f"[INFO] players.txt contiene {len(players)} jugadores")

    mapa = load_map(MAP_PATH)
    print(f"[INFO] mapa_cuentas.json contiene {len(mapa)} grupos")

    USERS_INDEX_COLL.drop()
    print("[INFO] L0_users_index reiniciada")

    docs = []
    now = now_utc()
    personas_usadas = set()

    # =============================
    # 1. Construir grupos del mapa
    # =============================
    for persona, cuentas in mapa.items():
        riot_ids = []
        puuids = []

        for rid in cuentas:
            puuid = get_puuid_from_api(rid)
            if puuid:
                riot_ids.append(rid)
                puuids.append(puuid)
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
                "created_at": now,
                "updated_at": now,
            })

            personas_usadas.update(riot_ids)

    # =============================
    # 2. Jugadores individuales
    # =============================
    for rid in players:
        if rid in personas_usadas:
            continue

        puuid = get_puuid_from_api(rid)
        if not puuid:
            print(f"[WARN] No se encontro PUUID para {rid}")
            continue

        persona = rid.split("#", 1)[0]

        docs.append({
            "_id": persona,
            "persona": persona,
            "riotIds": [rid],
            "puuids": [puuid],
            "created_at": now,
            "updated_at": now,
        })

    # =============================
    # Insert final
    # =============================
    if docs:
        USERS_INDEX_COLL.insert_many(docs)
        print(f"[DONE] Insertados {len(docs)} usuarios en L0_users_index")
    else:
        print("[WARN] No se insertaron documentos")

    print("[DONE] L0_build_users_index.py finalizado")


if __name__ == "__main__":
    main()
