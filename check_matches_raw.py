#!/usr/bin/env python3
import sys
import json
from pathlib import Path
from datetime import datetime, time
import argparse

# Add src to path to import utils
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from utils.db import get_mongo_client
from utils.config import MONGO_DB, COLLECTION_RAW_MATCHES, COLLECTION_USERS_INDEX

def load_persona_accounts(persona_name):
    """Loads accounts for a persona from mapa_cuentas.json."""
    map_path = ROOT / "mapa_cuentas.json"
    if not map_path.exists():
        print(f"Error: No se encontró {map_path}")
        return []
    
    with open(map_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    # Búsqueda insensible a mayúsculas
    persona_key = next((k for k in data.keys() if k.lower() == persona_name.lower()), None)
    if not persona_key:
        print(f"Error: Persona '{persona_name}' no encontrada en mapa_cuentas.json")
        return []
        
    return data[persona_key]

def get_puuid_by_riot_id(db, riot_id):
    """Finds PUUID for a given Riot ID (Name#Tag) in users_index."""
    query = {
        "$or": [
            {"accounts.riotId": {"$regex": f"^{riot_id}$", "$options": "i"}},
            {"riotIds": {"$regex": f"^{riot_id}$", "$options": "i"}}
        ]
    }
    doc = db[COLLECTION_USERS_INDEX].find_one(query)
    if not doc:
        return None
    
    # Format accounts[]
    for acc in doc.get("accounts", []):
        if acc.get("riotId", "").lower() == riot_id.lower():
            return acc.get("puuid")
    
    # Old format
    puuids = doc.get("puuids", [])
    if puuids:
        return puuids[0]
    
    return None

def check_matches_raw(start_date_str, end_date_str, persona_name, queue_id=None, min_friends=None, users_collection=COLLECTION_USERS_INDEX):
    try:
        dt_start = datetime.strptime(start_date_str, "%Y-%m-%d")
        dt_end = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError:
        print(f"Error: Formato de fecha inválido. Use YYYY-MM-DD.")
        return

    if dt_start > dt_end:
        print("Error: La fecha de inicio no puede ser posterior a la de fin.")
        return

    accounts = load_persona_accounts(persona_name)
    if not accounts:
        return

    # Timestamp range (ms)
    start_ts = int(datetime.combine(dt_start, time.min).timestamp() * 1000)
    end_ts = int(datetime.combine(dt_end, time.max).timestamp() * 1000)

    with get_mongo_client() as client:
        db = client[MONGO_DB]
        
        # Obtener todos los puuids de amigos para el filtro min_friends
        friend_puuids = set()
        if min_friends:
            all_users = list(db[users_collection].find({}, {"puuids": 1}))
            for u in all_users:
                friend_puuids.update(u.get("puuids", []))

        print(f"Persona: {persona_name}")
        print(f"Rango:   {start_date_str} -> {end_date_str}")
        if queue_id: print(f"Queue:   {queue_id}")
        if min_friends: print(f"Min F.:  {min_friends} (using {users_collection})")
        print(f"Buscando PUUIDs...")
        
        puuids = []
        for acc in accounts:
            p = get_puuid_by_riot_id(db, acc)
            if p:
                puuids.append(p)
                print(f"  - {acc} -> {p}")
        
        if not puuids:
            print("Error: No se encontró ningún PUUID válido.")
            return
        
        # Build query
        query = {
            "data.info.gameStartTimestamp": {"$gte": start_ts, "$lte": end_ts},
            "data.info.participants.puuid": {"$in": puuids}
        }
        if queue_id:
            query["data.info.queueId"] = queue_id
        
        matches = list(db[COLLECTION_RAW_MATCHES].find(query))
        
        if not matches:
            print(f"No se encontraron partidas para {persona_name} en este rango.")
            return
        
        print(f"\nAnalizando {len(matches)} partidas encontradas en Mongo...")
        
        positions = {}
        processed_match_ids = set()
        matches_filtered = 0

        for m in matches:
            match_id = m.get("_id")
            if match_id in processed_match_ids: continue
            processed_match_ids.add(match_id)

            info = m.get("data", {}).get("info", {})
            participants = info.get("participants", [])
            
            # Filtro por min_friends si se solicita
            if min_friends:
                actual_participants_puuids = [p.get("puuid") for p in participants]
                friends_in_game = [p for p in actual_participants_puuids if p in friend_puuids]
                if len(friends_in_game) < min_friends:
                    continue
            
            matches_filtered += 1
            for p_puuid in puuids:
                player_data = next((p for p in participants if p.get("puuid") == p_puuid), None)
                if player_data:
                    pos = player_data.get("teamPosition") or player_data.get("individualPosition") or "UNKNOWN"
                    if not pos or pos == "": pos = "UNKNOWN"
                    positions[pos] = positions.get(pos, 0) + 1
                    break
        
        print(f"Partidas que cumplen criterios: {matches_filtered}")
        print("\nResumen por posición:")
        print("-" * 30)
        for pos, count in sorted(positions.items()):
            print(f"{pos:15}: {count} partidas")
        print("-" * 30)

if __name__ == "__main__":
    from utils.config import QUEUE_FLEX, COLLECTION_USERS_INDEX
    parser = argparse.ArgumentParser(description="Cuenta partidas por posición de una PERSONA en un RANGO DE FECHAS (desde RAW Mongo).")
    parser.add_argument("inicio", help="Fecha inicio YYYY-MM-DD")
    parser.add_argument("fin",    help="Fecha fin YYYY-MM-DD")
    parser.add_argument("persona", help="Nombre de la persona en mapa_cuentas.json")
    parser.add_argument("--queue", type=int, default=QUEUE_FLEX, help="Filtrar por queueId")
    parser.add_argument("--min", type=int, help="Filtrar por mínimo de amigos presentes")
    parser.add_argument("--users-collection", type=str, default=COLLECTION_USERS_INDEX, help="Colección de usuarios (L0_users_index o L0_users_index_season)")
    
    args = parser.parse_args()
    check_matches_raw(args.inicio, args.fin, args.persona, queue_id=args.queue, min_friends=args.min, users_collection=args.users_collection)
