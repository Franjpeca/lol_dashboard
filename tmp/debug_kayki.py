from pathlib import Path
import sys
import json

# Asegurar que src/ esté en sys.path
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parents[1]
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utils.db import get_mongo_client
from utils.config import MONGO_DB

def debug_account(riot_id):
    print(f"--- DEBUG: {riot_id} ---")
    with get_mongo_client() as client:
        db = client[MONGO_DB]
        
        # 1. Buscar en el índice
        u = db["L0_users_index_season"].find_one({"accounts.riotId": riot_id})
        if not u:
            print(f"❌ No se encontró la cuenta en L0_users_index_season")
            # Probar en la normal por si acaso
            u = db["L0_users_index"].find_one({"accounts.riotId": riot_id})
            if u:
                print(f"⚠️ Encontrada en L0_users_index (normal), pero no en season.")
            else:
                return

        acc = next((a for a in u.get("accounts", []) if a["riotId"] == riot_id), None)
        puuid = acc.get("puuid") if acc else None
        print(f"PUUID en índice: {puuid}")

        if not puuid:
            print("❌ El PUUID está vacío en la base de datos.")
            return

        # 2. Buscar partidas con ese PUUID
        count_new = db["L0_matches"].count_documents({"metadata.participants": puuid})
        count_old = db["L0_matches"].count_documents({"data.metadata.participants": puuid})
        
        print(f"Partidas encontradas (estructura nueva): {count_new}")
        print(f"Partidas encontradas (estructura vieja): {count_old}")

if __name__ == "__main__":
    debug_account("Kayki マ#b0nk")
