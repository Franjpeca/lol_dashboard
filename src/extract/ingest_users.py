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
def get_modes():
    modes = {
        "villaquesitos": {
            "map_file": BASE_DIR / "data" / "mapa_cuentas.json",
            "collection": "L0_users_index",
            "drop_on_start": True
        },
        "season": {
            "map_file": BASE_DIR / "data" / "mapa_cuentas_season.json",
            "collection": "L0_users_index_season",
            "drop_on_start": False  # Upsert para season
        }
    }
    
    # Escanear otros archivos mapa_cuentas_XXX.json
    data_dir = BASE_DIR / "data"
    for f in data_dir.glob("mapa_cuentas_*.json"):
        suffix = f.stem.replace("mapa_cuentas_", "")
        if suffix == "season": continue
        
        modes[suffix] = {
            "map_file": f,
            "collection": f"L0_users_index_{suffix}",
            "drop_on_start": True
        }
    return modes

MODES = get_modes()


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def load_map(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_puuid_from_api(riot: RiotWatcher, riot_id: str, regional: str, max_retries: int = 3) -> str | None:
    """Obtiene PUUID desde Riot API con reintentos."""
    if "#" not in riot_id:
        print(f"  [WARN] Formato de Riot ID inválido (falta #): {riot_id}")
        return None
        
    name, tag = riot_id.split("#", 1)
    
    for attempt in range(1, max_retries + 1):
        try:
            acc = riot.account.by_riot_id(regional, name.strip(), tag.strip())
            
            # Rate limit friendly sleep
            if SLEEP_BETWEEN_CALLS > 0:
                print(".", end="", flush=True) 
                time.sleep(SLEEP_BETWEEN_CALLS)
                
            return acc["puuid"]
        except Exception as e:
            # Si es un 404 real, el nombre no existe, no reintentamos
            if "404" in str(e):
                print(f"  [ERROR] Cuenta no encontrada (404): {riot_id}")
                return None
                
            if attempt < max_retries:
                wait = 2 ** attempt
                print(f"  [RETRY] Error obteniendo {riot_id} (intento {attempt}/{max_retries}). Reintentando en {wait}s... {e}")
                time.sleep(wait)
            else:
                print(f"  [ERROR] No se pudo obtener PUUID para {riot_id} tras {max_retries} intentos: {e}")
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
            print(f"👤 Persona: {persona}")
            if not isinstance(cuentas, list):
                print(f"  [WARN] Omitiendo {persona}: cuentas no es una lista")
                continue

            riot_ids, puuids, accounts = [], [], []

            for rid in cuentas:
                print(f"  🔍 Buscando {rid}...", end=" ", flush=True)
                puuid = get_puuid_from_api(riot, rid, regional)
                if puuid:
                    print("✅ OK")
                    riot_ids.append(rid)
                    puuids.append(puuid)
                    accounts.append({"riotId": rid, "puuid": puuid})
                else:
                    print("❌ NO ENCONTRADO")

            if not puuids:
                print(f"  ⚠️  [ALERTA] {persona} se ha quedado con 0 PUUIDs válidos.")

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
                doc["created_at"] = now
                docs_to_insert.append(doc)
                inserted += 1
            else:
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

        total_final = coll.count_documents({})
        personas_vacias = coll.count_documents({"puuids": {"$size": 0}})

        print(f"[DONE] {inserted} insertados, {updated} actualizados | total en {collection_name}: {total_final}")
        if personas_vacias > 0:
            print(f"❌ [CRÍTICO] Hay {personas_vacias} personas sin ningún ID válido en esta colección.")
        
        # [FIX] Sincronización final de seguridad (asegurarnos de que puuids y accounts coinciden)
        print(f"[SYNC] Ejecutando sincronización de seguridad para {collection_name}...")
        users_after = list(coll.find())
        for u in users_after:
            acc_list = u.get('accounts', [])
            p_list = set(u.get('puuids', []))
            all_from_acc = {a['puuid'] for a in acc_list if 'puuid' in a}
            
            missing = all_from_acc - p_list
            if missing:
                coll.update_one({'_id': u['_id']}, {'$addToSet': {'puuids': {'$each': list(missing)}}})
        print(f"[SYNC] {collection_name} sincronizada correctamente.")


def main():
    parser = argparse.ArgumentParser(description="Construye índice de usuarios en MongoDB")
    # Hacemos que los modos sean dinámicos basados en los archivos encontrados
    parser.add_argument("--mode", choices=list(MODES.keys()) + ["all", "normal"], default="all",
                        help="Modo de ejecución (default: all)")
    args = parser.parse_args()

    regional = REGIONAL_ROUTING
    api_key = get_api_key(regional)
    riot = RiotWatcher(api_key)

    if args.mode == "all":
        # Usamos una lista estática para asegurar orden o procesar todos los detectados
        available_modes = get_modes()
        for m, c in available_modes.items():
            process_mode(m, c, riot, regional)
    else:
        # Intentar encontrar el modo específico
        available_modes = get_modes()
        if args.mode in available_modes:
            process_mode(args.mode, available_modes[args.mode], riot, regional)
        elif args.mode == "normal": # Compatibilidad
            process_mode("villaquesitos", available_modes["villaquesitos"], riot, regional)
        else:
            print(f"Error: Modo '{args.mode}' no reconocido.")


if __name__ == "__main__":
    main()
