import os
import json
import argparse
import sys
from pathlib import Path
from datetime import datetime
from date_utils import date_to_timestamp_ms
from dotenv import load_dotenv
from pymongo import MongoClient

sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))
DEFAULT_POOL = "ac89fa8d"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

RESULTS_ROOT = Path("data/results")
RUNTIME_ROOT = Path("data/runtime")


def auto_select_l1(queue, min_friends, pool_id=None):
    prefix = f"L1_q{queue}_min{min_friends}_"
    cands = [c for c in db.list_collection_names() if c.startswith(prefix)]

    if pool_id:
        pool_tag = f"pool_{pool_id}"
        cands = [c for c in cands if pool_tag in c]

    if not cands:
        return None
    cands.sort()
    return cands[-1]



def get_users_index_collection(pool_id: str):
    """Returns the correct L0_users_index collection name based on pool_id."""
    return "L0_users_index"


def extract_pool_from_l1(l1_name):
    return "pool_" + l1_name.split("_pool_", 1)[1]


def calculate_player_stats(l1_collection, pool_id: str, start_date=None, end_date=None):

    coll = db[l1_collection]

    # Filtro temporal opcional
    if start_date and end_date:
        ts_start = date_to_timestamp_ms(start_date, end_of_day=False)
        ts_end = date_to_timestamp_ms(end_date, end_of_day=True)
        match_query = {
            "data.info.gameStartTimestamp": {"$gte": ts_start, "$lte": ts_end}
        }
    else:
        match_query = {}

    # Mapeo puuid → persona
    collection_name = get_users_index_collection(pool_id)
    puuid_to_persona = {}
    for row in db[collection_name].find({}, {"persona": 1, "puuids": 1}):
        persona = row["persona"]
        for pid in row.get("puuids", []):
            puuid_to_persona[pid] = persona

    user_stats = {}

    cursor = coll.find(match_query, {
        "data.info.participants": 1,
        "data.info.gameId": 1,
        "data.info.gameStartTimestamp": 1,
        "friends_present": 1
    })

    for doc in cursor:
        info = doc.get("data", {}).get("info", {})
        participants = info.get("participants", [])
        friends_present = doc.get("friends_present", [])

        for p in participants:
            pid = p.get("puuid")
            if pid not in friends_present:
                continue

            persona = puuid_to_persona.get(pid)
            if not persona:
                continue

            if persona not in user_stats:
                user_stats[persona] = {
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
            dmg = p.get("totalDamageDealtToChampions", 0)
            taken = p.get("totalDamageTaken", 0)
            vision = p.get("visionScore", 0)
            game_id = info.get("gameId", "")

            s = user_stats[persona]

            s["kills"] += kills
            s["deaths"] += deaths
            s["assists"] += assists
            s["gold"] += gold
            s["damage_dealt"] += dmg
            s["damage_taken"] += taken
            s["vision_score"] += vision
            s["games"] += 1

            if kills > s["max_kills"]["kills"]:
                s["max_kills"] = {"kills": kills, "match_id": game_id}
            if deaths > s["max_deaths"]["deaths"]:
                s["max_deaths"] = {"deaths": deaths, "match_id": game_id}
            if assists > s["max_assists"]["assists"]:
                s["max_assists"] = {"assists": assists, "match_id": game_id}

    return user_stats


def save_stats(stats, dataset_folder, l1_name, start_date, end_date):
    out_file = dataset_folder / (
        f"metrics_05_players_stats_{start_date}_to_{end_date}.json"
        if start_date and end_date else "metrics_05_players_stats.json"
    )

    formatted = {}

    for persona, s in stats.items():
        g = max(s["games"], 1)  # evitar división por cero
        formatted[persona] = {
            "avg_kda": (s["kills"] + s["assists"]) / s["deaths"] if s["deaths"] > 0 else 0,
            "avg_kills": s["kills"] / g,
            "avg_deaths": s["deaths"] / g,
            "avg_assists": s["assists"] / g,
            "avg_gold": s["gold"] / g,
            "avg_damage_dealt": s["damage_dealt"] / g,
            "avg_damage_taken": s["damage_taken"] / g,
            "avg_vision_score": s["vision_score"] / g,
            "games": s["games"],
            "max_kills": s["max_kills"],
            "max_deaths": s["max_deaths"],
            "max_assists": s["max_assists"],
        }

    out_file.parent.mkdir(parents=True, exist_ok=True)

    # ESTRUCTURA CORRECTA TIPO M01/M04
    json_data = {
        "source_L1": l1_name,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "start_date": start_date,
        "end_date": end_date,
        "players": formatted
    }

    with out_file.open("w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE)
    parser.add_argument("--min", type=int, default=DEFAULT_MIN)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--pool", type=str, default=None)
    args = parser.parse_args()

    queue = args.queue
    min_f = args.min
    start = args.start
    end = args.end
    pool_arg = args.pool

    l1_name = auto_select_l1(queue, min_f, pool_arg)
    if not l1_name:
        print(f"[05] No L1 collection found for q{queue} min{min_f} pool={pool_arg}")
        return

    pool_id = extract_pool_from_l1(l1_name)

    if start and end:
        folder = RUNTIME_ROOT / pool_id / f"q{queue}" / f"min{min_f}"
    else:
        folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_f}"

    print(f"[05] Starting using {l1_name}")

    stats = calculate_player_stats(l1_name, pool_id, start, end)
    save_stats(stats, folder, l1_name, start, end)

    print("[05] Ended")


if __name__ == "__main__":
    main()
