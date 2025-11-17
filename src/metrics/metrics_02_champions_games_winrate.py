# metrics_02_champions_games_winrate.py

import os
import json
import argparse
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

RESULTS_ROOT = Path("data/results")


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


def compute_metrics(l2_players, l2_enemies, dataset_folder):
    champ_games = defaultdict(int)
    champ_wins = defaultdict(int)

    enemy_games = defaultdict(int)
    enemy_wins = defaultdict(int)

    # players
    coll_p = db[l2_players]
    for doc in coll_p.find({}, {"championName": 1, "win": 1}):
        champ = doc.get("championName", "UNKNOWN")
        win = doc.get("win", False)
        champ_games[champ] += 1
        if win:
            champ_wins[champ] += 1

    # enemies
    coll_e = db[l2_enemies]
    for doc in coll_e.find({}, {"championName": 1, "win": 1}):
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

    out_path = dataset_folder / "metrics_02_champions_games_winrate.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "champions": champs,
                "enemy_champions": enemy_champs,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE)
    parser.add_argument("--min", type=int, default=DEFAULT_MIN)
    args = parser.parse_args()

    queue = args.queue
    min_friends = args.min

    print(f"[02] Starting ... using collection: L2_players_flat_q{queue}_min{min_friends}")
    
    l2_players = auto_select_l2(queue, min_friends)
    if not l2_players:
        return

    # derive enemy L2
    l2_enemies = l2_players.replace("L2_players_flat_", "L2_enemies_flat_")

    # derive L1
    l1_name = l2_to_l1(l2_players)
    pool_id = extract_pool_from_l1(l1_name)

    dataset_folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    dataset_folder.mkdir(parents=True, exist_ok=True)

    compute_metrics(l2_players, l2_enemies, dataset_folder)
    
    print(f"[02] Ended")


def run():
    main()


if __name__ == "__main__":
    run()
