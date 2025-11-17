# 01_metrics_players_games_winrate.py

import os
import json
import argparse
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

USERS_INDEX = db["L0_users_index"]
ACCOUNTS_COLL = db["riot_accounts"]

RESULTS_ROOT = Path("data/results")


def now_utc():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def load_puuid_to_user_mapping():
    mapping = {}
    cursor = USERS_INDEX.find({}, {"persona": 1, "puuids": 1})
    for doc in cursor:
        persona = doc["persona"]
        for puuid in doc["puuids"]:
            mapping[puuid] = persona
    return mapping


def resolve_riotId(puuid, riotId_l2):
    if riotId_l2 is not None:
        return riotId_l2

    acc_doc = ACCOUNTS_COLL.find_one({"puuid": puuid}, {"riotId": 1})
    if acc_doc:
        return acc_doc.get("riotId")

    return None


def auto_select_l2(queue, min_friends):
    prefix = f"L2_players_flat_q{queue}_min{min_friends}_"
    candidates = [c for c in db.list_collection_names() if c.startswith(prefix)]
    if not candidates:
        return None
    candidates.sort()
    return candidates[-1]


def l2_to_l1(l2_name):
    return l2_name.replace("L2_players_flat_", "L1_")


def extract_pool_from_l1(l1_name):
    # L1_q440_min5_pool_ab12cd34 -> pool_ab12cd34
    return "pool_" + l1_name.split("_pool_", 1)[1]


def compute_for_collection(coll_name, puuid_to_user, dataset_folder):
    coll_src = db[coll_name]

    pipeline = [
        {
            "$group": {
                "_id": "$puuid",
                "games": {"$sum": 1},
                "wins": {"$sum": {"$cond": ["$win", 1, 0]}},
                "riotId": {"$first": "$riotId"}
            }
        },
        {
            "$project": {
                "_id": 1,
                "riotId": 1,
                "games": 1,
                "wins": 1,
                "winrate": {
                    "$cond": [
                        {"$eq": ["$games", 0]},
                        0,
                        {"$divide": ["$wins", "$games"]}
                    ]
                }
            }
        }
    ]

    results_puuid = list(coll_src.aggregate(pipeline))

    if not results_puuid:
        return

    users = {}

    for r in results_puuid:
        puuid = r["_id"]
        persona = puuid_to_user.get(puuid)

        if persona is None:
            continue

        if persona not in users:
            users[persona] = {
                "persona": persona,
                "total_games": 0,
                "total_wins": 0,
                "winrate": 0,
                "puuids": {}
            }

        riot_id = resolve_riotId(puuid, r.get("riotId"))

        users[persona]["total_games"] += r["games"]
        users[persona]["total_wins"] += r["wins"]

        users[persona]["puuids"][puuid] = {
            "riotId": riot_id,
            "games": r["games"],
            "wins": r["wins"],
            "winrate": r["winrate"]
        }

    for u in users.values():
        if u["total_games"] == 0:
            u["winrate"] = 0
        else:
            u["winrate"] = u["total_wins"] / u["total_games"]

    global_winrate_mean = sum(u["winrate"] for u in users.values()) / len(users)

    output_json = {
        "source_L2": coll_name,
        "generated_at": now_utc(),
        "global_winrate_mean": global_winrate_mean,
        "users": list(users.values())
    }

    json_path = dataset_folder / "metrics_01_players_games_winrate.json"
    with json_path.open("w", encoding="utf-8") as f:
        json.dump(output_json, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE)
    parser.add_argument("--min", type=int, default=DEFAULT_MIN)
    args = parser.parse_args()

    queue = args.queue
    min_friends = args.min

    print(f"[01] Starting ... using collection: L2_players_flat_q{queue}_min{min_friends}")
    
    l2_name = auto_select_l2(queue, min_friends)
    if not l2_name:
        return

    l1_name = l2_to_l1(l2_name)
    pool_id = extract_pool_from_l1(l1_name)

    dataset_folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    dataset_folder.mkdir(parents=True, exist_ok=True)

    puuid_to_user = load_puuid_to_user_mapping()

    compute_for_collection(l2_name, puuid_to_user, dataset_folder)
    
    print(f"[01] Ended")


if __name__ == "__main__":
    main()
