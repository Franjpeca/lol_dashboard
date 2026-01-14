import sys
from pathlib import Path

# Asegura que 'src' esté en sys.path
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parents[2]        # lol_data/
SRC_DIR = BASE_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Ahora podemos importar 'utils.api_key_manager'
from utils.api_key_manager import get_api_key

# El resto de tu código sigue igual...
import os
import json
import datetime
import time
from dotenv import load_dotenv
from pymongo import MongoClient, errors
from riotwatcher import LolWatcher, RiotWatcher

BASE_DIR = Path(__file__).resolve().parents[1]  # carpeta src/
sys.path.append(str(BASE_DIR))

# ============================
# CONFIGURACION
# ============================
load_dotenv()

REGIONAL = os.getenv("REGIONAL_ROUTING", "europe")  # Usamos "europe" como valor predeterminado
API_KEY = get_api_key(REGIONAL)   # Pasamos la región como argumento
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
    # Encode properly for Windows console
    try:
        print(f"[{now_utc().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)
    except UnicodeEncodeError:
        # Fallback: encode to ASCII, replacing problematic characters
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
    """
    Get PUUID for a user. Returns (puuid, error_msg).
    error_msg is None if successful.
    """
    from riotwatcher import ApiError
    
    riot_id = f"{name}#{tag}"
    
    try:
        acc = riot.account.by_riot_id(REGIONAL, name, tag)
        return acc["puuid"], None
    except ApiError as e:
        status = e.response.status_code
        if status == 404:
            # TRY FALLBACK: Check DB for previous identities
            # riot_accounts collection holds {riot_id, puuid, ...}
            # We want to find if 'riot_id' WAS this user.
            # But the current DB structure is simple {riot_id, puuid, region}.
            # We will improve it to store "history".
            # For now, let's look for the exact riot_id in the existing DB.
            existing = None
            try:
                existing = accounts_col.find_one({"riot_id": riot_id})
                # Or checks 'previous_identities' if we had them.
                # Since we don't have them yet, we check the current 'riot_id' field.
                # If we find it in DB, it means valid history.
            except Exception:
                pass
            
            if existing and "puuid" in existing:
                log(f"[INFO] User '{riot_id}' not found in API (404), but found in DB cache. Using cached PUUID.")
                return existing["puuid"], None

            return None, f"User '{riot_id}' NOT FOUND (404). Please verify the username in mapa_cuentas.json"
        elif status in (401, 403):
            return None, f"API Key REJECTED ({status}). Key is invalid or expired."
        elif status == 429:
            return None, f"Rate limit exceeded (429) for '{riot_id}'. Too many requests."
        else:
            return None, f"API Error {status} for '{riot_id}': {str(e)}"
    except Exception as e:
        return None, f"Network/Unknown error for '{riot_id}': {str(e)}"

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
    """
    Inserts or updates account in MongoDB.
    Maintains a history of 'previous_identities' if the name changes for the same PUUID.
    """
    try:
        # Check if PUUID exists
        existing = coll_accounts.find_one({"puuid": puuid})
        
        update_data = {
            "puuid": puuid,
            "riot_id": riot_id,
            "region": region,
            "last_updated": now_utc().isoformat()
        }
        
        if existing:
            # Logic for name history
            old_name = existing.get("riot_id")
            if old_name and old_name != riot_id:
                # Name changed!
                # Add old name to history
                history = existing.get("previous_identities", [])
                if old_name not in history:
                    history.append(old_name)
                update_data["previous_identities"] = history
                log(f"[INFO] Name change detected: {old_name} -> {riot_id}")
            else:
                # Keep existing history
                update_data["previous_identities"] = existing.get("previous_identities", [])
        else:
            # New account, set initial added_at
            update_data["added_at"] = now_utc().isoformat()
        
        coll_accounts.update_one(
            {"puuid": puuid},
            {"$set": update_data},
            upsert=True
        )
    except Exception as e:
        log(f"[WARN] Error guardando cuenta en Mongo: {e}")

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

    # --- Get all Riot IDs from L0_users_index ---
    L0_index = db["L0_users_index"]
    all_riot_ids = []
    
    for doc in L0_index.find({}, {"riotIds": 1}):
        riot_ids = doc.get("riotIds", [])
        all_riot_ids.extend(riot_ids)
    
    if not all_riot_ids:
        log("❌ No se encontraron usuarios en L0_users_index")
        return

        puuid, error = get_puuid(name, tag)
        # We already validated, so this should not fail, but check anyway
        if not puuid or error:
            log(f"[!] UNEXPECTED ERROR for {riot_id}: {error}")
            raise RuntimeError(f"Unexpected error during registration: {error}")
        
        upsert_account(riot_id, puuid, REGIONAL)
        riotid_map[puuid] = (riot_id, REGIONAL)
        known_puuids.add(puuid)
        log(f"[OK] {riot_id} registrado o actualizado en riot_accounts.")
    
    log("[SYNC] [OK] Todos los usuarios verificados y registrados correctamente.\n")

    # ================================================================
    # Descargamos partidas de TODOS los puuids en L0_users_index
    # ================================================================
    L0_index = db["L0_users_index"]
    all_user_docs = list(L0_index.find({}, {"puuids": 1, "persona": 1}))
    all_puuids = []
    for doc in all_user_docs:
        for p in doc.get("puuids", []):
            all_puuids.append((doc["persona"], p))

    log(f"[INFO] Descargando partidas de {len(all_puuids)} PUUID registrados en L0_users_index\n")

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
                    upsert_account(riot_name, pid, reg)
                else:
                    unknown_puuids.add(pid)

            time.sleep(SLEEP)

        log(f"📊 {persona} ({puuid}) -> nuevas: {total_inserted}, omitidas: {total_skipped}")

    if unknown_puuids:
        log(f"⚠️  Se ignoraron {len(unknown_puuids)} PUUID desconocidos (jugadores ajenos).")
    log("✅ Finalizado.")

if __name__ == "__main__":
    main()