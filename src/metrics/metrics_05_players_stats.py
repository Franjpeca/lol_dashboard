import os
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

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
    """Extrae el pool de un nombre de colección L1 (ej. 'L1_q440_min5_pool_ab12cd34' -> 'pool_ab12cd34')."""
    return "pool_" + l1_name.split("_pool_", 1)[1]


def calculate_player_stats(l1_collection):
    """Calcula las estadísticas de los jugadores (kills, deaths, assists, etc.) desde una colección L1."""
    coll = db[l1_collection]
    
    # Cargar mapeo de puuid -> persona desde L0_users_index
    l0_coll = db["L0_users_index"]
    puuid_to_persona = {}
    for user in l0_coll.find({}, {"_id": 1, "persona": 1, "puuids": 1}):
        persona = user.get("persona", "Unknown")
        for puuid in user.get("puuids", []):
            puuid_to_persona[puuid] = persona

    user_stats = {}

    cursor = coll.find(
        {},
        {
            "data.info.participants": 1,
            "data.info.gameStartTimestamp": 1,
            "data.info.gameId": 1,
            "friends_present": 1
        }
    )

    for doc in cursor:
        info = doc.get("data", {}).get("info", {}) or {}
        participants = info.get("participants", [])
        friends_present = doc.get("friends_present", [])

        for p in participants:
            pid = p.get("puuid")
            if pid not in friends_present:
                continue  # Solo jugadores presentes en la partida

            if pid not in user_stats:
                persona = puuid_to_persona.get(pid, "Unknown")
                user_stats[pid] = {
                    "name": persona,
                    "kills": 0,
                    "deaths": 0,
                    "assists": 0,
                    "gold": 0,
                    "damage_dealt": 0,
                    "damage_taken": 0,
                    "vision_score": 0,
                    "games": 0,
                    "max_kills": {"kills": 0, "match_id": ""},
                    "max_deaths": {"deaths": 0, "match_id": ""},
                    "max_assists": {"assists": 0, "match_id": ""},
                }

            kills = p.get("kills", 0)
            deaths = p.get("deaths", 0)
            assists = p.get("assists", 0)
            gold = p.get("goldEarned", 0)
            damage_dealt = p.get("totalDamageDealtToChampions", 0)
            damage_taken = p.get("totalDamageTaken", 0)
            vision_score = p.get("visionScore", 0)
            game_id = doc.get("data", {}).get("info", {}).get("gameId", "")

            # Sumar los valores
            user_stats[pid]["kills"] += kills
            user_stats[pid]["deaths"] += deaths
            user_stats[pid]["assists"] += assists
            user_stats[pid]["gold"] += gold
            user_stats[pid]["damage_dealt"] += damage_dealt
            user_stats[pid]["damage_taken"] += damage_taken
            user_stats[pid]["vision_score"] += vision_score
            user_stats[pid]["games"] += 1

            # Actualizar máximos
            if kills > user_stats[pid]["max_kills"]["kills"]:
                user_stats[pid]["max_kills"] = {"kills": kills, "match_id": game_id}
            if deaths > user_stats[pid]["max_deaths"]["deaths"]:
                user_stats[pid]["max_deaths"] = {"deaths": deaths, "match_id": game_id}
            if assists > user_stats[pid]["max_assists"]["assists"]:
                user_stats[pid]["max_assists"] = {"assists": assists, "match_id": game_id}

    return user_stats


def save_stats_to_file(stats, pool_id, queue, min_friends):
    out_path = RESULTS_ROOT / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}" / "metrics_05_players_stats.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Formatear los resultados
    formatted_stats = {}
    for pid, stats_data in stats.items():
        persona = stats_data.get("name", "Unknown")  # Aseguramos que el nombre sea correcto, por defecto "Unknown"
        formatted_stats[persona] = {
            "avg_kda": (stats_data["kills"] + stats_data["assists"]) / stats_data["deaths"] if stats_data["deaths"] != 0 else 0,
            "avg_kills": stats_data["kills"] / stats_data["games"],
            "avg_deaths": stats_data["deaths"] / stats_data["games"],
            "avg_assists": stats_data["assists"] / stats_data["games"],
            "avg_gold": stats_data["gold"] / stats_data["games"],
            "avg_damage_dealt": stats_data["damage_dealt"] / stats_data["games"],
            "avg_damage_taken": stats_data["damage_taken"] / stats_data["games"],
            "avg_vision_score": stats_data["vision_score"] / stats_data["games"],
            "max_kills": stats_data["max_kills"],
            "max_deaths": stats_data["max_deaths"],
            "max_assists": stats_data["max_assists"],
        }

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(formatted_stats, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Calcular estadísticas de jugadores en LoL")
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE, help="Queue ID (ej. 440 para Flex)")
    parser.add_argument("--min", type=int, default=DEFAULT_MIN, help="Mínimo de amigos por partida")
    parser.add_argument("--pool", type=str, default=DEFAULT_POOL, help="Pool ID (ej. ac89fa8d)")
    args = parser.parse_args()

    queue = args.queue
    min_friends = args.min
    pool_id = args.pool

    print(f"[05] Starting ... using collection: L1_q{queue}_min{min_friends}_pool_{pool_id}")
    
    # Detectar colección L1 correspondiente
    l1_name = auto_select_l1(queue, min_friends)
    if not l1_name:
        return

    # Calcular estadísticas
    player_stats = calculate_player_stats(l1_name)

    # Guardar estadísticas en un archivo
    save_stats_to_file(player_stats, pool_id, queue, min_friends)
    
    print(f"[05] Ended")


def run():
    main()


if __name__ == "__main__":
    run()
