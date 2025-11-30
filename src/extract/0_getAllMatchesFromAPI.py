import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))
sys.stdout.reconfigure(encoding='utf-8')

import os
import json
import datetime
import time
from dotenv import load_dotenv
from pymongo import MongoClient, errors
from riotwatcher import LolWatcher, RiotWatcher

from utils.api_key_manager import get_api_key

BASE_DIR = Path(__file__).resolve().parents[1]  # carpeta src/
sys.path.append(str(BASE_DIR))

# ============================
# CONFIGURACION
# ============================
load_dotenv()


REGIONAL = os.getenv("REGIONAL_ROUTING", "europe")
API_KEY = get_api_key()
QUEUE_FLEX = int(os.getenv("QUEUE_FLEX", "440"))
COUNT_PER_PLAYER = int(os.getenv("COUNT_PER_PLAYER", "800"))
SLEEP = float(os.getenv("SLEEP_BETWEEN_CALLS", "0.2"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "6"))

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
COLL_MATCHES = os.getenv("MONGO_COLLECTION_RAW_MATCHES", "L0_all_raw_matches")
COLL_ACCOUNTS = "riot_accounts"

USERS_DIR = Path(r"C:\Users\Diazr\Documents\ficheros_escritorio\lol_analisis\data\usuarios")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
coll_matches = db[COLL_MATCHES]
coll_accounts = db[COLL_ACCOUNTS]

lol = LolWatcher(API_KEY, timeout=REQUEST_TIMEOUT)
riot = RiotWatcher(API_KEY, timeout=REQUEST_TIMEOUT)

# ============================
# UTILIDADES
# ============================
def now_utc():
    return datetime.datetime.now(datetime.UTC)

def log(msg):
    print(f"[{now_utc().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

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

def parse_player(line: str):
    s = line.strip()
    if not s or s.startswith("#"):
        return None
    if "#" in s:
        a, b = s.split("#", 1)
        return a.strip(), b.strip()
    if "," in s:
        a, b = s.split(",", 1)
        return a.strip(), b.strip()
    return None

def read_players(path: Path):
    players = []
    if not path.exists():
        return players
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            p = parse_player(line)
            if p:
                players.append(p)
    return players

def get_puuid(name, tag):
    try:
        acc = riot.account.by_riot_id(REGIONAL, name, tag)
        return acc["puuid"]
    except Exception:
        acc = safe_call(riot.account.by_riot_id, REGIONAL, name, tag)
        return acc["puuid"] if acc else None

def iter_match_ids(puuid):
    start = 0
    batch = 100
    while True:
        ids = safe_call(lol.match.matchlist_by_puuid, REGIONAL, puuid,
                        start=start, count=batch, queue=QUEUE_FLEX)
        if not ids:
            break
        for mid in ids:
            yield mid
        start += len(ids)
        if len(ids) < batch:
            break

# ============================
# INSERCIONES A MONGO
# ============================
def upsert_account(riot_id, puuid, region):
    coll_accounts.update_one(
        {"puuid": puuid},
        {"$set": {
            "riotId": riot_id,
            "puuid": puuid,
            "region": region,
            "updated_at": now_utc()
        },
         "$setOnInsert": {
            "added_at": now_utc()
        }},
        upsert=True
    )

def insert_match(match_json, region):
    match_id = match_json.get("metadata", {}).get("matchId")
    if not match_id:
        return False
    doc = {
        "_id": match_id,
        "inserted_at": now_utc(),
        "source": "riot_api",
        "region": region,
        "data": match_json
    }
    try:
        coll_matches.insert_one(doc)
        return True
    except errors.DuplicateKeyError:
        return False
    except Exception as e:
        log(f"[ERROR] insert {match_id}: {e}")
        return False

# ============================
# MAIN
# ============================
def main():
    # --- Sincronizar colección riot_accounts desde data/usuarios ---
    riotid_map = {}
    if USERS_DIR.exists():
        log("[SYNC] Sincronizando colección riot_accounts desde data/usuarios...")
        for f in USERS_DIR.glob("*.json"):
            d = read_json(f)
            if not d:
                continue
            riot_id = d.get("riotId")
            puuid = d.get("puuid")
            region = d.get("region", REGIONAL)
            if not puuid:
                continue
            upsert_account(riot_id, puuid, region)
            riotid_map[puuid] = (riot_id, region)
        log("[SYNC] Sincronización inicial completada.\n")
    else:
        log("[SYNC] No se encontró carpeta data/usuarios, se omite sincronización inicial.\n")

    known_puuids = set(riotid_map.keys())
    unknown_puuids = set()

    # --- Leer jugadores de players.txt (solo para registrar cuentas nuevas) ---
    players = read_players(Path("players.txt"))
    if not players:
        log("❌ No se encontraron jugadores en players.txt")
        return

    log(f"[SYNC] Verificando que todos los jugadores de players.txt estén registrados...")
    for name, tag in players:
        riot_id = f"{name}#{tag}"
        puuid = get_puuid(name, tag)
        if not puuid:
            log(f"[WARN] No se pudo obtener PUUID para {riot_id}")
            continue
        upsert_account(riot_id, puuid, REGIONAL)
        riotid_map[puuid] = (riot_id, REGIONAL)
        known_puuids.add(puuid)
        log(f"[OK] {riot_id} registrado o actualizado en riot_accounts.")
    log("[SYNC] Verificación completada.\n")

    # ================================================================
    # 🟩 NUEVO: Descargamos partidas de TODOS los puuids en L0_users_index
    # ================================================================
    L0_index = db["L0_users_index"]
    all_user_docs = list(L0_index.find({}, {"puuids": 1, "persona": 1}))
    all_puuids = []
    for doc in all_user_docs:
        for p in doc.get("puuids", []):
            all_puuids.append((doc["persona"], p))

    log(f"[INFO] Descargando partidas de {len(all_puuids)} PUUID registrados en L0_users_index")

    # --- Descargar partidas por cada PUUID ---
    for persona, puuid in all_puuids:
        log(f"\n=== Persona {persona} -> PUUID {puuid} ===")
        total_inserted = 0
        total_skipped = 0

        for match_id in iter_match_ids(puuid):
            if coll_matches.find_one({"_id": match_id}):
                total_skipped += 1
                continue

            match_json = safe_call(lol.match.by_id, REGIONAL, match_id)
            if not match_json:
                continue

            if insert_match(match_json, match_id.split("_", 1)[0]):
                total_inserted += 1
                log(f"✔ Insertada {match_id}")
            else:
                total_skipped += 1

            # Actualizamos riot_accounts con participantes conocidos
            participants = (match_json.get("metadata") or {}).get("participants", [])
            for pid in participants:
                if pid in known_puuids:
                    riot_name, reg = riotid_map.get(pid, (None, REGIONAL))
                    upsert_account(pid, riot_name, reg)
                else:
                    unknown_puuids.add(pid)

            time.sleep(SLEEP)

        log(f"📊 {persona} ({puuid}) -> nuevas: {total_inserted}, omitidas: {total_skipped}")

    if unknown_puuids:
        log(f"⚠️  Se ignoraron {len(unknown_puuids)} PUUID desconocidos (jugadores ajenos).")
    log("✅ Finalizado.")

if __name__ == "__main__":
    main()