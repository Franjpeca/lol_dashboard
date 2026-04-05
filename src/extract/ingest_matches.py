"""
ingest_matches.py
Unifica la extracción/ingesta de partidas crudas hacia MongoDB.
Soporta dos orígenes de datos:
  --source api  (Por defecto) Descarga de la API de Riot basándose en el índice de usuarios.
  --source file Lee archivos JSON locales (caché) y los inserta en BD.

Uso:
    python extract/ingest_matches.py --source api
    python extract/ingest_matches.py --source file
"""

import sys
import json
import time
import argparse
import logging
import datetime
from pathlib import Path
from pymongo import errors
from riotwatcher import LolWatcher

# Asegurar que src/ esté en sys.path
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parents[2]  # lol_data/
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utils.api_key_manager import get_api_key
from utils.config import (
    MONGO_DB,
    COLLECTION_RAW_MATCHES,
    COLLECTION_ACCOUNTS,
    COLLECTION_USERS_INDEX,
    PATH_LOL_CACHE,
    PATH_LOL_USERS,
    PATH_LOL_PLAYERS,
    REGIONAL_ROUTING,
    QUEUE_FLEX,
    SLEEP_BETWEEN_CALLS,
    REQUEST_TIMEOUT,
    MAX_RETRIES
)
from utils.db import get_mongo_client

# ============================
# LOGGING Y UTILIDADES
# ============================
def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)

def log(msg):
    try:
        print(f"[{now_utc().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)
    except UnicodeEncodeError:
        safe_msg = msg.encode('ascii', 'replace').decode('ascii')
        print(f"[{now_utc().strftime('%Y-%m-%d %H:%M:%S')}] {safe_msg}", flush=True)

def read_json(path: Path):
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def safe_call(fn, *args, **kwargs):
    attempt = 0
    while True:
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            attempt += 1
            r = getattr(e, "response", None)
            sc = getattr(r, "status_code", None)
            retry_after = r.headers.get("Retry-After") if r else None
            log(f"[WARN] {fn.__name__} intento={attempt} status={sc} retry_after={retry_after}")
            if sc == 429 and retry_after:
                try:
                    time.sleep(float(retry_after))
                except:
                    time.sleep(2)
            else:
                time.sleep(min(60, 2 ** attempt))
            if attempt >= MAX_RETRIES:
                log(f"[ERROR] {fn.__name__} abandonado tras {attempt} intentos: {e}")
                return None

# ============================
# BD COMUNES
# ============================
def upsert_account(db, riot_id, puuid, region):
    """Inserta o actualiza cuenta en MongoDB guardando historial de nombres si cambia."""
    try:
        coll_accounts = db[COLLECTION_ACCOUNTS]
        existing = coll_accounts.find_one({"puuid": puuid})
        
        update_data = {
            "puuid": puuid,
            "region": region,
            "updated_at": now_utc()
        }
        
        if riot_id:
            update_data["riotId"] = riot_id
            if existing and existing.get("riotId") != riot_id:
                # Guardar en histórico si cambió
                old_name = existing.get("riotId")
                coll_accounts.update_one(
                    {"puuid": puuid},
                    {"$addToSet": {"history": {"riotId": old_name, "date": now_utc()}}}
                )
        
        coll_accounts.update_one(
            {"puuid": puuid},
            {"$set": update_data, "$setOnInsert": {"created_at": now_utc()}},
            upsert=True
        )
    except Exception as e:
        log(f"[ERROR] upsert_account: {e}")

def insert_match(db, match_json, region, source):
    """Inserta partida en MongoDB evitando duplicados."""
    try:
        match_id = match_json["metadata"]["matchId"]
        match_json["_id"] = match_id
        match_json["ingested_at"] = now_utc()
        match_json["source_ext"] = source
        match_json["region_ext"] = region
        
        db[COLLECTION_RAW_MATCHES].insert_one(match_json)
        return True
    except errors.DuplicateKeyError:
        return False
    except Exception as e:
        log(f"[ERROR] insert_match: {e}")
        return False

def sync_accounts_from_local(db):
    """Sincroniza la colección riot_accounts desde data/usuarios local."""
    riotid_map = {}
    if PATH_LOL_USERS and PATH_LOL_USERS.exists():
        log("[SYNC] Sincronizando colección riot_accounts desde data/usuarios...")
        for f in PATH_LOL_USERS.glob("*.json"):
            d = read_json(f)
            if not d:
                continue
            riot_id = d.get("riotId")
            puuid = d.get("puuid")
            region = d.get("region", REGIONAL_ROUTING)
            if not puuid:
                continue
            upsert_account(db, riot_id, puuid, region)
            if riot_id:
                riotid_map[puuid] = (riot_id, region)
        log("[SYNC] Sincronización inicial completada.\n")
    return riotid_map

# ============================
# MODO: API
# ============================
def iter_match_ids(lol, puuid, max_total=None):
    start = 0
    batch = 100
    yielded = 0
    while True:
        # Si hay límite, ajustar el batch para no pedir de más
        current_batch = batch
        if max_total is not None:
            remaining = max_total - yielded
            if remaining <= 0:
                break
            current_batch = min(batch, remaining)

        ids = safe_call(lol.match.matchlist_by_puuid, REGIONAL_ROUTING, puuid,
                        start=start, count=current_batch, queue=QUEUE_FLEX)
        if not ids:
            break
        
        print(f" (analizando {start}-{start+len(ids)}...)", end="", flush=True)
        for mid in ids:
            yield mid
            yielded += 1
            if max_total is not None and yielded >= max_total:
                return

        start += len(ids)
        if len(ids) < current_batch:
            break

def ingest_from_api(db, users_collections=["L0_users_index", "L0_users_index_season"], limit=15):
    riotid_map = sync_accounts_from_local(db)
    known_puuids = set(riotid_map.keys())
    unknown_puuids = set()

    lol = LolWatcher(get_api_key(REGIONAL_ROUTING), timeout=REQUEST_TIMEOUT)

    # Identificar de quién vamos a descargar (Unión de todas las colecciones)
    all_puuids_map = {} # puuid -> persona
    
    for coll_name in users_collections:
        L0_index = db[coll_name]
        try:
            docs = list(L0_index.find({}, {"puuids": 1, "_id": 1}))
            for doc in docs:
                persona = doc["_id"]
                for p in doc.get("puuids", []):
                    all_puuids_map[p] = persona
        except Exception as e:
            log(f"[WARN] Error leyendo colección {coll_name}: {e}")

    if not all_puuids_map:
        log(f"❌ No se encontraron usuarios en las colecciones: {users_collections}")
        return

    unique_puuids = list(all_puuids_map.items())
    log(f"[INFO] Descargando partidas de {len(unique_puuids)} jugadores únicos detectados en {users_collections}\n")

    for puuid, persona in unique_puuids:
        log(f"\n=== Jugador {persona} -> PUUID {puuid} ===")
        total_inserted = 0
        total_skipped = 0

        for match_id in iter_match_ids(lol, puuid, max_total=limit):
            if db[COLLECTION_RAW_MATCHES].find_one({"_id": match_id}):
                total_skipped += 1
                if total_skipped % 50 == 0:
                    print(".", end="", flush=True) 
                continue

            match_json = safe_call(lol.match.by_id, REGIONAL_ROUTING, match_id)
            if not match_json:
                continue

            if insert_match(db, match_json, match_id.split("_", 1)[0], "riot_api"):
                total_inserted += 1
                log(f"✔ Insertada {match_id}")
            else:
                total_skipped += 1

            participants = (match_json.get("metadata") or {}).get("participants", [])
            for pid in participants:
                if pid in known_puuids:
                    riot_name, reg = riotid_map.get(pid, (None, REGIONAL_ROUTING))
                    upsert_account(db, riot_name, pid, reg)
                else:
                    unknown_puuids.add(pid)

            # Imprimir un punto discreto para indicar que sigue trabajando durante la pausa
            if total_inserted % 2 == 0:
                print(".", end="", flush=True)
            time.sleep(SLEEP_BETWEEN_CALLS)

        log(f"📊 {persona} -> nuevas: {total_inserted}, omitidas: {total_skipped}")

    if unknown_puuids:
        log(f"⚠️  Se ignoraron {len(unknown_puuids)} PUUID desconocidos (jugadores ajenos).")
    log("✅ Finalizado ingesta desde API.")

# ============================
# MODO: FILE
# ============================
def iter_match_files(base: Path):
    for region_dir in base.iterdir():
        if not region_dir.is_dir():
            continue
        for f in region_dir.glob("*.json"):
            yield f, region_dir.name

def ingest_from_file(db):
    if not PATH_LOL_CACHE or not PATH_LOL_CACHE.exists():
        log(f"❌ No existe la carpeta de entrada caché local (LOL_CACHE_DIR)")
        return

    riotid_map = sync_accounts_from_local(db)
    known_puuids = set(riotid_map.keys())

    total_files = 0
    inserted = 0
    skipped = 0
    total_accounts = 0
    unknown_puuids = set()

    for file_path, region in iter_match_files(PATH_LOL_CACHE):
        total_files += 1
        data = read_json(file_path)
        if not data:
            skipped += 1
            continue

        match_id = data.get("metadata", {}).get("matchId")
        participants = (data.get("metadata") or {}).get("participants", [])
        if not match_id or not participants:
            skipped += 1
            continue

        if insert_match(db, data, region, str(file_path)):
            inserted += 1
        else:
            skipped += 1

        for puuid in participants:
            if puuid not in known_puuids:
                unknown_puuids.add(puuid)
                continue
            riot_id, reg = riotid_map.get(puuid, (None, region))
            upsert_account(db, riot_id, puuid, reg)
            total_accounts += 1

    log(f"\n📦 Total ficheros: {total_files}")
    log(f"✅ Insertados nuevos: {inserted}")
    log(f"⚠️  Duplicados o inválidos: {skipped}")
    log(f"👤 Cuentas conocidas actualizadas: {total_accounts}")
    if unknown_puuids:
        log(f"⚠️  Se ignoraron {len(unknown_puuids)} PUUID desconocidos (jugadores ajenos).")
    log("✅ Finalizado ingesta desde archivos.")

# ============================
# MAIN
# ============================
def main():
    parser = argparse.ArgumentParser(description="Ingesta de partidas desde Riot API")
    parser.add_argument("--source", choices=["api", "file"], default="api")
    parser.add_argument("--mode", choices=["normal", "season", "all"], default="all")
    parser.add_argument("--limit", type=int, default=15, help="Límite de partidas por jugador (por defecto 15)")
    parser.add_argument("--all", action="store_true", help="Descargar TODO el historial (ignora --limit)")
    args = parser.parse_args()

    # Si --all, el límite es infinito (None)
    final_limit = None if args.all else args.limit

    with get_mongo_client() as client:
        db = client[MONGO_DB]
        
        if args.source == "api":
            if args.mode == "normal":
                cols = ["L0_users_index"]
            elif args.mode == "season":
                cols = ["L0_users_index_season"]
            else:
                cols = ["L0_users_index", "L0_users_index_season"]
            
            ingest_from_api(db, users_collections=cols, limit=final_limit)
        else:
            # Modo file (legacy/debug)
            ingest_from_file(db)


if __name__ == "__main__":
    main()
