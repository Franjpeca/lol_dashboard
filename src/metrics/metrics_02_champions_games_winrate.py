# metrics_02_champions_games_winrate.py

import os
import json
import argparse
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

RESULTS_ROOT = Path("data/results")
RUNTIME_ROOT = Path("data/runtime")


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


def compute_metrics(l2_players, l2_enemies, dataset_folder, start_date=None, end_date=None):

    if start_date and end_date:
        start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59).timestamp() * 1000)

        match_filter = {
            "gameStartTimestamp": {"$gte": start_ts, "$lte": end_ts}
        }
    else:
        match_filter = {}

    champ_games = defaultdict(int)
    champ_wins = defaultdict(int)

    enemy_games = defaultdict(int)
    enemy_wins = defaultdict(int)

    coll_p = db[l2_players]
    cursor_p = coll_p.find(match_filter, {"championName": 1, "win": 1})

    for doc in cursor_p:
        champ = doc.get("championName", "UNKNOWN")
        win = doc.get("win", False)
        champ_games[champ] += 1
        if win:
            champ_wins[champ] += 1

    coll_e = db[l2_enemies]
    cursor_e = coll_e.find(match_filter, {"championName": 1, "win": 1})

    for doc in cursor_e:
        champ = doc.get("championName", "UNKNOWN")
        win = doc.get("win", False)
        enemy_games[champ] += 1
        if win:
            enemy_wins[champ] += 1

    champs = []
    for champ, games in champ_games.items():
        wins = champ_wins[champ]
        wr = wins / games * 100 if games else 0
        champs.append({
            "champion": champ,
            "games": games,
            "winrate": round(wr, 2),
        })

    enemy_champs = []
    for champ, games in enemy_games.items():
        wins = enemy_wins[champ]
        wr = wins / games * 100 if games else 0
        enemy_champs.append({
            "champion": champ,
            "games": games,
            "winrate": round(wr, 2),
        })

    champs.sort(key=lambda x: x["games"], reverse=True)
    enemy_champs.sort(key=lambda x: x["games"], reverse=True)

    output_json = {
        "start_date": start_date if start_date else None,
        "end_date": end_date if end_date else None,
        "champions": champs,
        "enemy_champions": enemy_champs,
    }

    dataset_folder.mkdir(parents=True, exist_ok=True)

    if start_date and end_date:
        filename = f"metrics_02_champions_games_winrate_{start_date}_to_{end_date}.json"
    else:
        filename = "metrics_02_champions_games_winrate.json"

    out_path = dataset_folder / filename

    with out_path.open("w", encoding="utf-8") as f:
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

    print(f"[02] Starting ... using L2 for queue={queue} min={min_friends}")

    l2_players = auto_select_l2(queue, min_friends)
    if not l2_players:
        return

    l2_enemies = l2_players.replace("L2_players_flat_", "L2_enemies_flat_")

    l1_name = l2_to_l1(l2_players)
    pool_id = extract_pool_from_l1(l1_name)

    if start_date and end_date:
        dataset_folder = RUNTIME_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
        print("Guardado en:", dataset_folder)
    else:
        dataset_folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"

    compute_metrics(l2_players, l2_enemies, dataset_folder, start_date, end_date)

    print(f"[02] Ended")


if __name__ == "__main__":
    main()
