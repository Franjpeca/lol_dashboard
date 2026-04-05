
import os
from dotenv import load_dotenv
from pymongo import MongoClient
import json

load_dotenv()
client = MongoClient(os.getenv('MONGO_URI'))
db = client[os.getenv('MONGO_DB')]

MATCH_ID = "EUW1_7810469776"

def debug():
    print(f"--- Depurando partida {MATCH_ID} ---")
    
    # 1. Cargar amigos
    friend_puuids = set()
    users = list(db['L0_users_index'].find())
    for u in users:
        for p in u.get('puuids', []):
            friend_puuids.add(p)
    
    print(f"Total PUUIDs conocidos: {len(friend_puuids)}")

    # 2. Buscar partida en L0
    match = db['L0_all_raw_matches'].find_one({"_id": MATCH_ID})
    if not match:
        print("❌ Partida NO encontrada en L0_all_raw_matches")
        return

    # 3. Simular filtrado
    metadata = match.get('metadata', {})
    participants = metadata.get('participants', [])
    print(f"Participantes en metadata: {len(participants)}")
    
    friends_found = [p for p in participants if p in friend_puuids]
    print(f"Amigos detectados: {len(friends_found)}")
    for f in friends_found:
        # Buscar a quién pertenece
        owner = db['L0_users_index'].find_one({"puuids": f})
        print(f" - Amigo: {f} (Pertenece a: {owner['_id'] if owner else 'Desconocido'})")

    if len(friends_found) >= 4:
        print("✅ La partida DEBERÍA pasar el filtro (min 4)")
    else:
        print(f"❌ La partida NO pasa el filtro (encontrados {len(friends_found)}, buscamos 4)")

if __name__ == "__main__":
    debug()
