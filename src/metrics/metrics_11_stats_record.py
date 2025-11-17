import os
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import timedelta

# Cargar variables de entorno
load_dotenv()

# Conexión a MongoDB
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))
DEFAULT_POOL = "ac89fa8d"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

RESULTS_ROOT = Path("data/results")


def auto_select_l1(queue, min_friends):
    """Selecciona la colección L1 más reciente según los parámetros queue y min_friends."""
    prefix = f"L1_q{queue}_min{min_friends}_"
    cands = [c for c in db.list_collection_names() if c.startswith(prefix)]
    if not cands:
        return None
    cands.sort()
    return cands[-1]


def extract_pool_from_l1(l1_name):
    """Extrae el pool de un nombre de colección L1."""
    return "pool_" + l1_name.split("_pool_", 1)[1]


def format_game_duration(seconds):
    """Convierte segundos a formato MM:SS"""
    if seconds == 0:
        return "0m 0s"
    minutes = seconds // 60
    seconds = seconds % 60
    return f"{minutes}m {seconds}s"


def calculate_record_stats(l1_collection):
    """Calcula los records (máximos) de cada jugador desde una colección L1."""
    coll = db[l1_collection]
    
    # Cargar mapeo de puuid -> persona desde L0_users_index
    l0_coll = db["L0_users_index"]
    puuid_to_persona = {}
    for user in l0_coll.find({}, {"_id": 1, "persona": 1, "puuids": 1}):
        persona = user.get("persona", "Unknown")
        for puuid in user.get("puuids", []):
            puuid_to_persona[puuid] = persona

    # Estructura: persona -> records
    player_records = {}

    cursor = coll.find(
        {},
        {
            "data.info.participants": 1,
            "data.info.gameDuration": 1,
            "data.info.gameId": 1,
            "friends_present": 1
        }
    )

    for doc in cursor:
        info = doc.get("data", {}).get("info", {}) or {}
        participants = info.get("participants", [])
        friends_present = doc.get("friends_present", [])
        game_duration = info.get("gameDuration", 0)  # en segundos
        game_id = info.get("gameId", "")

        for p in participants:
            pid = p.get("puuid")
            if pid not in friends_present:
                continue  # Solo jugadores tracked

            persona = puuid_to_persona.get(pid, "Unknown")

            if persona not in player_records:
                player_records[persona] = {
                    "max_kills": {"value": 0, "game_id": ""},
                    "max_deaths": {"value": 0, "game_id": ""},
                    "max_assists": {"value": 0, "game_id": ""},
                    "max_vision_score": {"value": 0, "game_id": ""},
                    "max_farm": {"value": 0, "game_id": ""},
                    "max_damage_dealt": {"value": 0, "game_id": ""},
                    "max_gold": {"value": 0, "game_id": ""},
                    "longest_game": {"value": "0m 0s", "game_id": ""}
                }

            kills = p.get("kills", 0)
            deaths = p.get("deaths", 0)
            assists = p.get("assists", 0)
            vision_score = p.get("visionScore", 0)
            farm = p.get("totalMinionsKilled", 0)
            damage_dealt = p.get("totalDamageDealtToChampions", 0)
            gold = p.get("goldEarned", 0)

            # Actualizar records con información de la partida
            if kills > player_records[persona]["max_kills"]["value"]:
                player_records[persona]["max_kills"] = {"value": kills, "game_id": game_id}

            if deaths > player_records[persona]["max_deaths"]["value"]:
                player_records[persona]["max_deaths"] = {"value": deaths, "game_id": game_id}

            if assists > player_records[persona]["max_assists"]["value"]:
                player_records[persona]["max_assists"] = {"value": assists, "game_id": game_id}

            if vision_score > player_records[persona]["max_vision_score"]["value"]:
                player_records[persona]["max_vision_score"] = {"value": round(vision_score, 2), "game_id": game_id}

            if farm > player_records[persona]["max_farm"]["value"]:
                player_records[persona]["max_farm"] = {"value": farm, "game_id": game_id}

            if damage_dealt > player_records[persona]["max_damage_dealt"]["value"]:
                player_records[persona]["max_damage_dealt"] = {"value": damage_dealt, "game_id": game_id}

            if gold > player_records[persona]["max_gold"]["value"]:
                player_records[persona]["max_gold"] = {"value": gold, "game_id": game_id}

            # Actualizar partida más larga
            current_longest_ms = int(player_records[persona]["longest_game"]["value"].split("m ")[0]) * 60
            if "s" in player_records[persona]["longest_game"]["value"]:
                current_longest_ms += int(player_records[persona]["longest_game"]["value"].split("m ")[1].split("s")[0])
            
            if game_duration > current_longest_ms:
                player_records[persona]["longest_game"] = {"value": format_game_duration(game_duration), "game_id": game_id}

    return player_records


def save_stats_to_file(stats, pool_id, queue, min_friends):
    """Guarda los records en un archivo JSON."""
    out_path = RESULTS_ROOT / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}" / "metrics_11_stats_record.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Calcular records de jugadores en LoL")
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE, help="Queue ID (ej. 440 para Flex)")
    parser.add_argument("--min", type=int, default=DEFAULT_MIN, help="Mínimo de amigos por partida")
    parser.add_argument("--pool", type=str, default=DEFAULT_POOL, help="Pool ID (ej. ac89fa8d)")
    args = parser.parse_args()

    queue = args.queue
    min_friends = args.min
    pool_id = args.pool

    print(f"[11] Starting ... using collection: L1_q{queue}_min{min_friends}_pool_{pool_id}")
    
    # Detectar colección L1 correspondiente
    l1_name = auto_select_l1(queue, min_friends)
    if not l1_name:
        return

    # Calcular records
    player_records = calculate_record_stats(l1_name)

    # Guardar en archivo
    save_stats_to_file(player_records, pool_id, queue, min_friends)
    
    print(f"[11] Ended")


def run():
    main()


if __name__ == "__main__":
    run()
