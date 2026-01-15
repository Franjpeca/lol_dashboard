import os
import json
import datetime
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient, errors

# ============================
# CONFIG
# ============================
load_dotenv()

DATA_BASE = Path(r"C:\Users\Diazr\Documents\ficheros_escritorio\lol_analisis\data\cache_partidas")
USERS_DIR = Path(r"C:\Users\Diazr\Documents\ficheros_escritorio\lol_analisis\data\usuarios")
PLAYERS_FILE = Path(r"C:\Users\Diazr\Documents\ficheros_escritorio\lol_analisis\players.txt")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://root:root123@localhost:27017/lol_data")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
COLLECTION_MATCHES = os.getenv("MONGO_COLLECTION_RAW_MATCHES", "L0_all_raw_matches")
COLLECTION_ACCOUNTS = "riot_accounts"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
coll_matches = db[COLLECTION_MATCHES]
coll_accounts = db[COLLECTION_ACCOUNTS]

# ============================
# FUNCIONES
# ============================
def read_json(path: Path):
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def iter_match_files(base: Path):
    for region_dir in base.iterdir():
        if not region_dir.is_dir():
            continue
        for f in region_dir.glob("*.json"):
            yield f, region_dir.name

def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)

def upsert_account(puuid: str, riot_id: str = None, region: str = "unknown"):
    """Inserta o actualiza una cuenta de Riot en la colección riot_accounts."""
    update_doc = {
        "puuid": puuid,
        "region": region,
        "updated_at": now_utc(),
    }
    if riot_id:
        update_doc["riotId"] = riot_id

    coll_accounts.update_one(
        {"puuid": puuid},
        {"$set": update_doc, "$setOnInsert": {"added_at": now_utc()}},
        upsert=True
    )

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

# ============================
# MAIN
# ============================
def main():
    if not DATA_BASE.exists():
        print(f"❌ No existe la carpeta de entrada: {DATA_BASE}")
        return

    # --- Sincronizar colección riot_accounts desde data/usuarios ---
    if USERS_DIR.exists():
        print("[SYNC] Sincronizando colección riot_accounts desde data/usuarios...")
        for f in USERS_DIR.glob("*.json"):
            d = read_json(f)
            if not d:
                continue
            riot_id = d.get("riotId")
            puuid = d.get("puuid")
            region = d.get("region", "unknown")
            if not puuid:
                continue
            coll_accounts.update_one(
                {"puuid": puuid},
                {
                    "$set": {
                        "riotId": riot_id,
                        "region": region,
                        "updated_at": now_utc(),
                    },
                    "$setOnInsert": {"added_at": now_utc()},
                },
                upsert=True,
            )
        print("[SYNC] Sincronización inicial completada.\n")
    else:
        print("[SYNC] No se encontró carpeta data/usuarios, se omite sincronización inicial.\n")

    # --- Cargar mapa de riotIds conocidos ---
    riotid_map = {}
    if USERS_DIR.exists():
        for f in USERS_DIR.glob("*.json"):
            d = read_json(f)
            if not d:
                continue
            riotid = d.get("riotId")
            puuid = d.get("puuid")
            region = d.get("region", "unknown")
            if riotid and puuid:
                riotid_map[puuid] = (riotid, region)
    known_puuids = set(riotid_map.keys())

    # --- Asegurar jugadores de players.txt ---
    if PLAYERS_FILE.exists():
        print("[SYNC] Verificando que todos los jugadores de players.txt estén registrados...")
        players = read_players(PLAYERS_FILE)
        for name, tag in players:
            riot_id = f"{name}#{tag}"
            # Buscar su archivo en data/usuarios
            for f in USERS_DIR.glob("*.json"):
                d = read_json(f)
                if not d:
                    continue
                if d.get("riotId") == riot_id:
                    puuid = d.get("puuid")
                    region = d.get("region", "unknown")
                    upsert_account(puuid, riot_id, region)
                    known_puuids.add(puuid)
                    print(f"[OK] {riot_id} sincronizado en riot_accounts.")
                    break
        print("[SYNC] Verificación completada.\n")
    else:
        print("[SYNC] No se encontró players.txt, se omite registro directo de jugadores.\n")

    # --- Procesar partidas ---
    total_files = 0
    inserted = 0
    skipped = 0
    total_accounts = 0
    unknown_puuids = set()

    for file_path, region in iter_match_files(DATA_BASE):
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

        # --- Insertar partida si no existe ---
        if not coll_matches.find_one({"_id": match_id}):
            doc = {
                "_id": match_id,
                "inserted_at": now_utc(),
                "source_file": str(file_path),
                "region": region,
                "data": data
            }
            try:
                coll_matches.insert_one(doc)
                inserted += 1
            except errors.DuplicateKeyError:
                skipped += 1
            except Exception as e:
                print(f"[ERROR] {file_path.name}: {e}")
                skipped += 1
        else:
            skipped += 1

        # --- Actualizar solo cuentas conocidas ---
        for puuid in participants:
            if puuid not in known_puuids:
                unknown_puuids.add(puuid)
                continue
            riot_id, reg = riotid_map[puuid]
            upsert_account(puuid, riot_id, reg)
            total_accounts += 1

    # --- Resumen final ---
    print(f"\n📦 Total ficheros: {total_files}")
    print(f"✅ Insertados nuevos: {inserted}")
    print(f"⚠️  Duplicados o inválidos: {skipped}")
    print(f"👤 Cuentas conocidas actualizadas: {total_accounts}")
    if unknown_puuids:
        print(f"⚠️  Se ignoraron {len(unknown_puuids)} PUUID desconocidos (jugadores ajenos).")
    print(f"📚 Colección de partidas: {DB_NAME}.{COLLECTION_MATCHES}")
    print(f"📚 Colección de cuentas: {DB_NAME}.{COLLECTION_ACCOUNTS}")

if __name__ == "__main__":
    main()
