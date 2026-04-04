"""
ingest_users.py
Construye el índice de usuarios en MongoDB desde mapa_cuentas.json.

Uso:
    python extract/ingest_users.py                   # Modo normal → L0_users_index
    python extract/ingest_users.py --mode season     # Modo season → L0_users_index_season
"""

import sys
import json
import argparse
import datetime
import time
from pathlib import Path

# Asegurar que src/ esté en sys.path
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parents[2]  # lol_data/
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from riotwatcher import RiotWatcher
from utils.api_key_manager import get_api_key
from utils.config import (
    MONGO_DB, COLLECTION_USERS_INDEX, REGIONAL_ROUTING,
    SLEEP_BETWEEN_CALLS
)
from utils.db import get_mongo_client

# ================================
# CONFIG POR MODO
# ================================
MODES = {
    "normal": {
        "map_file": BASE_DIR / "data" / "mapa_cuentas.json",
        "collection": COLLECTION_USERS_INDEX,   # L0_users_index
        "drop_on_start": True,                   # Reemplazar índice completo
    },
    "season": {
        "map_file": BASE_DIR / "data" / "mapa_cuentas_season.json",
        "collection": "L0_users_index_season",
        "drop_on_start": False,                  # Upsert: mantener histórico
    },
}


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def load_map(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_puuid_from_api(riot: RiotWatcher, riot_id: str, regional: str) -> str | None:
    """Obtiene PUUID siempre desde Riot API."""
    try:
        name, tag = riot_id.split("#", 1)
        acc = riot.account.by_riot_id(regional, name, tag)
        
        # Rate limit
        if SLEEP_BETWEEN_CALLS > 0:
            time.sleep(SLEEP_BETWEEN_CALLS)
            
        return acc["puuid"]
    except Exception as e:
        print(f"  [WARN] No se pudo obtener PUUID para {riot_id}: {e}")
        return None


def process_mode(mode_name, cfg, riot, regional):
    collection_name = cfg["collection"]
    map_path = cfg["map_file"]
    drop_on_start = cfg["drop_on_start"]

    print(f"\n[BOOT] ingest_users.py | modo={mode_name} | colección={collection_name}")

    mapa = load_map(map_path)
    if not mapa:
        print(f"[ERROR] Fichero no encontrado o vacío: {map_path}")
        return

    print(f"[INFO] {map_path.name}: {len(mapa)} personas")

    now = datetime.datetime.now(datetime.timezone.utc)
    inserted = 0
    updated = 0

    with get_mongo_client() as client:
        db = client[MONGO_DB]
        coll = db[collection_name]

        if drop_on_start:
            coll.drop()
            print(f"[INFO] {collection_name} reiniciada")

        docs_to_insert = []

        for persona, cuentas in mapa.items():
            if not isinstance(cuentas, list):
                print(f"[WARN] Skipping {persona}: accounts is not a list")
                continue

            riot_ids, puuids, accounts = [], [], []

            for rid in cuentas:
                puuid = get_puuid_from_api(riot, rid, regional)
                if puuid:
                    riot_ids.append(rid)
                    puuids.append(puuid)
                    accounts.append({"riotId": rid, "puuid": puuid})

            if not riot_ids:
                print(f"[WARN] {persona}: ninguna cuenta válida, omitiendo")
                continue

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

            if drop_on_start:
                # Inserción directa (drop ya hecho)
                doc["created_at"] = now
                docs_to_insert.append(doc)
                inserted += 1
            else:
                # Upsert: actualiza si existe, inserta si no
                result = coll.update_one(
                    {"_id": persona},
                    {"$set": doc, "$setOnInsert": {"created_at": now}},
                    upsert=True
                )
                if result.upserted_id:
                    inserted += 1
                elif result.modified_count > 0:
                    updated += 1

        if docs_to_insert:
            coll.insert_many(docs_to_insert)

        total = coll.count_documents({})

    print(f"[DONE] {inserted} insertados, {updated} actualizados | total en {collection_name}: {total}")


def main():
    parser = argparse.ArgumentParser(description="Construye índice de usuarios en MongoDB")
    parser.add_argument("--mode", choices=["normal", "season", "all"], default="all",
                        help="Modo de ejecución (default: all)")
    args = parser.parse_args()

    regional = REGIONAL_ROUTING
    api_key = get_api_key(regional)
    riot = RiotWatcher(api_key)

    if args.mode == "all":
        for m_name, m_cfg in MODES.items():
            process_mode(m_name, m_cfg, riot, regional)
    else:
        process_mode(args.mode, MODES[args.mode], riot, regional)


if __name__ == "__main__":
    main()
