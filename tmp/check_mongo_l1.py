
import os
import sys
import argparse
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

# Rutas
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parents[1]
sys.path.insert(0, str(BASE_DIR / "src"))

load_dotenv(dotenv_path=BASE_DIR / ".env")

def check_l1(riot_id, min_friends, pool="villaquesitos"):
    try:
        from utils.config import MONGO_URI, MONGO_DB
    except:
        MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        MONGO_DB = os.getenv("MONGO_DB", "lol_data")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    
    # 1. Obtener PUUID
    user_doc = db["L0_users_index"].find_one({"accounts.riotId": riot_id})
    if not user_doc:
        print(f"❌ Error: La cuenta '{riot_id}' no está en el índice L0 de usuarios.")
        return
    
    puuid = None
    for acc in user_doc.get("accounts", []):
        if acc["riotId"] == riot_id:
            puuid = acc["puuid"]
            break
            
    if not puuid:
        print(f"❌ Error: No se encontró PUUID para {riot_id} dentro del documento de {user_doc['_id']}.")
        return
    
    # 2. Construir nombre de la colección L1
    # Formato: L1_q440_min{min}_pool_{pool}
    coll_name = f"L1_q440_min{min_friends}_pool_{pool}"
    print(f"\n🔍 Buscando en L1: {coll_name}")
    
    if coll_name not in db.list_collection_names():
        print(f"⚠️  La colección {coll_name} NO EXISTE en MongoDB.")
        return

    # 3. Buscar partidas
    cursor = db[coll_name].find({"metadata.participants": puuid}).sort("info.gameStartTimestamp", -1).limit(4)
    matches = list(cursor)
    
    print(f"--- [L1 - {coll_name}] Últimas 4 partidas ---")
    if not matches:
        print(f"🚫 NO se han encontrado partidas en esta capa L1.")
        print(f"Esto significa que la partida fue descartada por no cumplir el requisito de {min_friends} amigos.")
    else:
        for m in matches:
            ts = m.get("info", {}).get("gameStartTimestamp", 0) / 1000
            dt = datetime.fromtimestamp(ts) if ts > 0 else "N/A"
            print(f"ID: {m['_id']} | Fecha: {dt}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("riot_id")
    parser.add_argument("--min", type=int, default=5)
    parser.add_argument("--pool", default="villaquesitos")
    args = parser.parse_args()
    check_l1(args.riot_id, args.min, args.pool)
