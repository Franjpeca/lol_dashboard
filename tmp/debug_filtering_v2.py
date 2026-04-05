
import os
import re
from pymongo import MongoClient

# Carga manual de .env para evitar fallos de expansión de variables
def load_env_manual():
    env_vars = {}
    with open(".env", "r") as f:
        for line in f:
            if "=" in line and not line.startswith("#"):
                k, v = line.strip().split("=", 1)
                env_vars[k] = v
    return env_vars

env = load_env_manual()
# Construcción manual de la URI (evitando problemas de shell)
uri = f"mongodb://{env['MONGO_USER']}:{env['MONGO_PASS']}@localhost:27017/lol_data?authSource=admin"

client = MongoClient(uri, serverSelectionTimeoutMS=5000)
db = client['lol_data']
MATCH_ID = "EUW1_7810469776"

try:
    print(f"Probando conexión a {uri}...")
    db.command('ping')
    print("✅ Conexión establecida.")
    
    # 1. Cargar amigos
    friend_puuids = set()
    users = list(db['L0_users_index'].find())
    print(f"Cargadas {len(users)} personas del índice.")
    for u in users:
        for p in u.get('puuids', []):
            friend_puuids.add(p)
    
    # 2. Analizar partida
    match = db['L0_all_raw_matches'].find_one({"_id": MATCH_ID})
    if not match:
        print(f"❌ La partida {MATCH_ID} no está en L0.")
    else:
        participants = match.get('metadata', {}).get('participants', [])
        found = [p for p in participants if p in friend_puuids]
        print(f"Partida {MATCH_ID} encontrada.")
        print(f"Amigos en la partida: {len(found)}")
        for f in found:
            print(f" - {f}")
            
except Exception as e:
    print(f"❌ Error: {e}")
