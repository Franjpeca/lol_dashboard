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
RUNTIME_ROOT = Path("data/runtime")


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
    return "pool_" + l1_name.split("_pool_", 1)[1]


def compute_for_collection(coll_name, puuid_to_user, dataset_folder, start_date=None, end_date=None):
    coll_src = db[coll_name]

    if start_date and end_date:
        start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59).timestamp() * 1000)

        pipeline = [
            {
                "$match": {
                    "gameStartTimestamp": {"$gte": start_ts, "$lte": end_ts},
                }
            },
            {
                "$group": {
                    "_id": "$puuid",
                    "games": {"$sum": 1},
                    "wins": {"$sum": {"$cond": ["$win", 1, 0]}}
                }
            },
            {
                "$project": {
                    "_id": 1,
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
    else:
        pipeline = [
            {
                "$group": {
                    "_id": "$puuid",
                    "games": {"$sum": 1},
                    "wins": {"$sum": {"$cond": ["$win", 1, 0]}}
                }
            },
            {
                "$project": {
                    "_id": 1,
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
    total_games_global = 0
    total_wins_global = 0

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
                "winrate": 0.0,
                "puuids": {}
            }

        riot_id = resolve_riotId(puuid, r.get("riotId"))

        users[persona]["total_games"] += r["games"]
        users[persona]["total_wins"] += r["wins"]

        users[persona]["puuids"][puuid] = {
            "riotId": riot_id,
            "games": r["games"],
            "wins": r["wins"],
            "winrate": float(r["winrate"])
        }

        total_games_global += r["games"]
        total_wins_global += r["wins"]

    for persona, data in users.items():
        if data["total_games"] > 0:
            data["winrate"] = data["total_wins"] / data["total_games"]

    if total_games_global > 0:
        global_winrate_mean = total_wins_global / total_games_global
    else:
        global_winrate_mean = 0.0

    output_json = {
        "source_L2": coll_name,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "start_date": start_date if start_date else None,
        "end_date": end_date if end_date else None,
        "global_winrate_mean": global_winrate_mean,
        "users": list(users.values())
    }

    dataset_folder.mkdir(parents=True, exist_ok=True)

    if start_date and end_date:
        filename = f"metrics_01_players_games_winrate_{start_date}_to_{end_date}.json"
    else:
        filename = "metrics_01_players_games_winrate.json"

    json_path = dataset_folder / filename

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(output_json, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE)
    parser.add_argument("--min", type=int, default=DEFAULT_MIN)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    args = parser.parse_args()

    queue = args.queue
    min_friends = args.min
    start_date = args.start
    end_date = args.end

    print(f"[01] Starting ... using collection: L2_players_flat_q{queue}_min{min_friends}")

    l2_name = auto_select_l2(queue, min_friends)
    if not l2_name:
        return

    l1_name = l2_to_l1(l2_name)
    pool_id = extract_pool_from_l1(l1_name)

    if start_date and end_date:
        dataset_folder = RUNTIME_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
        print("Guardado en dataset_folder:", dataset_folder)
    else:
        dataset_folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"

    puuid_to_user = load_puuid_to_user_mapping()

    compute_for_collection(l2_name, puuid_to_user, dataset_folder, start_date, end_date)

    print(f"[01] Ended")


if __name__ == "__main__":
    main()
