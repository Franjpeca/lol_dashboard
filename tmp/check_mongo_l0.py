
import os
import sys
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

# Rutas
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parents[1]
sys.path.insert(0, str(BASE_DIR / "src"))

load_dotenv(dotenv_path=BASE_DIR / ".env")

# Obtener nombres de colección desde config si es posible, o hardcoded por seguridad
try:
    from utils.config import MONGO_URI, MONGO_DB, COLLECTION_RAW_MATCHES
except:
    MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    MONGO_DB = os.getenv("MONGO_DB", "lol_data")
    COLLECTION_RAW_MATCHES = "L0_all_raw_matches"

def check_l0(riot_id):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    
    # 1. Obtener PUUID de esta cuenta
    user_doc = db["L0_users_index"].find_one({"accounts.riotId": riot_id})
    if not user_doc:
        print(f"❌ Error: La cuenta '{riot_id}' no está en el índice L0 de usuarios.")
        return
    
    puuid = next(acc["puuid"] for acc in user_doc["accounts"] if acc["riotId"] == riot_id)
    print(f"PUUID encontrado: {puuid}")

    # 2. Buscar en la colección cruda (L0)
    print(f"\n--- [L0 - MongoDB] Últimas 5 partidas detectadas para este PUUID ---")
    
    # Buscamos en los participantes de los metadatos de las partidas crudas
    cursor = db[COLLECTION_RAW_MATCHES].find(
        {"metadata.participants": puuid}
    ).sort("info.gameStartTimestamp", -1).limit(5)
    
    matches = list(cursor)
    
    found = False
    for match in matches:
        found = True
        match_id = match.get("_id", "N/A")
        ts = match.get("info", {}).get("gameStartTimestamp", 0) / 1000
        dt = datetime.fromtimestamp(ts) if ts > 0 else "Fecha desconocida"
        print(f"ID: {match_id} | Fecha: {dt}")
        
    if not found:
        print("⚠️  No hay rastro de partidas para este jugador en la capa L0 (MongoDB).")
        print("Esto significa que 'ingest_matches.py' no las ha descargado todavía.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python tmp/check_mongo_l0.py \"Nombre#TAG\"")
        sys.exit(1)
    check_l0(sys.argv[1])
