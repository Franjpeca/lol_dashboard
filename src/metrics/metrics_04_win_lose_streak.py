import os
import json
import argparse
from pathlib import Path
from datetime import datetime
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


def now_utc():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ===============================
# L1 detection
# ===============================

def auto_select_l1(queue, min_friends):
    prefix = f"L1_q{queue}_min{min_friends}_"
    cands = [c for c in db.list_collection_names() if c.startswith(prefix)]
    if not cands:
        return None
    cands.sort()
    return cands[-1]


def extract_pool_from_l1(l1_name):
    # L1_q440_min5_pool_ab12cd34 -> pool_ab12cd34
    return "pool_" + l1_name.split("_pool_", 1)[1]


# ===============================
# Streak helpers
# ===============================

def compute_streaks(results):
    max_win = 0
    max_lose = 0
    cur_win = 0
    cur_lose = 0

    for r in results:
        if r == 1:
            cur_win += 1
            cur_lose = 0
        else:
            cur_lose += 1
            cur_win = 0

        if cur_win > max_win:
            max_win = cur_win

        if cur_lose > max_lose:
            max_lose = cur_lose

    if not results:
        current = 0
    else:
        current = 1 if results[-1] == 1 else -1

    return max_win, max_lose, current


# ===============================
# Main logic
# ===============================

def compute_metrics(l1_name, dataset_folder):
    coll = db[l1_name]

    cursor = coll.find(
        {},
        {
            "data.info.participants": 1,
            "data.info.gameStartTimestamp": 1,
            "friends_present": 1
        }
    )

    user_stats = {}

    for doc in cursor:
        info = doc.get("data", {}).get("info", {}) or {}
        ts = info.get("gameStartTimestamp", 0)
        friends_present = doc.get("friends_present", [])

        for pid in friends_present:

            if pid not in user_stats:
                user_stats[pid] = {
                    "records": [],  # Solo mantener las rachas
                    "games": 0
                }

            for p in info.get("participants", []):
                if p.get("puuid") == pid:

                    win = 1 if p.get("win") else 0

                    user_stats[pid]["records"].append((ts, win))
                    user_stats[pid]["games"] += 1

    final = {}

    # Now convert pid->persona using users_index
    users_index = {p["puuid"]: p["persona"] for p in db["L0_users_index"].aggregate([ 
        {"$unwind": "$puuids"},
        {"$project": {"puuid": "$puuids", "persona": "$persona"}}
    ])}

    for pid, stats in user_stats.items():
        persona = users_index.get(pid)
        if not persona:
            continue

        records = sorted(stats["records"], key=lambda x: x[0])
        results = [w for _, w in records]

        max_w, max_l, cur = compute_streaks(results)

        if persona not in final:
            final[persona] = {
                "max_win_streak": 0,
                "max_lose_streak": 0,
                "current_streak": 0
            }

        final[persona]["max_win_streak"] = max(final[persona]["max_win_streak"], max_w)
        final[persona]["max_lose_streak"] = max(final[persona]["max_lose_streak"], max_l)
        final[persona]["current_streak"] = cur

    out_path = dataset_folder / "metrics_04_win_lose_streak.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE)
    parser.add_argument("--min", type=int, default=DEFAULT_MIN)
    args = parser.parse_args()

    queue = args.queue
    min_friends = args.min


    
    l1_name = auto_select_l1(queue, min_friends)
    if not l1_name:
        return

    pool_id = extract_pool_from_l1(l1_name)

    print(f"[04] Starting ... using collection: L1_q{queue}_min{min_friends}_pool_{pool_id}")

    dataset_folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    dataset_folder.mkdir(parents=True, exist_ok=True)

    compute_metrics(l1_name, dataset_folder)
    
    print(f"[04] Ended")


def run():
    main()


if __name__ == "__main__":
    run()
