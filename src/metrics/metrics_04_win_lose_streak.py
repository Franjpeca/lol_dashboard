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

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

RESULTS_ROOT = Path("data/results")
RUNTIME_ROOT = Path("data/runtime")


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


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


def compute_streaks(win_list):
    max_win = max_lose = cur_win = cur_lose = 0

    for w in win_list:
        if w == 1:
            cur_win += 1
            cur_lose = 0
        else:
            cur_lose += 1
            cur_win = 0

        max_win = max(max_win, cur_win)
        max_lose = max(max_lose, cur_lose)

    if not win_list:
        current = 0
    else:
        current = 1 if win_list[-1] == 1 else -1

    return max_win, max_lose, current


def compute_metrics(l1_name, dataset_folder, pool_id: str, start_date=None, end_date=None):

    coll = db[l1_name]

    # Ventana temporal
    if start_date and end_date:
        ts_start = date_to_timestamp_ms(start_date, end_of_day=False)
        ts_end = date_to_timestamp_ms(end_date, end_of_day=True)
        query = {"data.info.gameStartTimestamp": {"$gte": ts_start, "$lte": ts_end}}
    else:
        query = {}

    cursor = coll.find(
        query,
        {
            "data.info.participants": 1,
            "data.info.gameStartTimestamp": 1,
            "friends_present": 1
        }
    )

    # PUUID → registros
    user_stats = {}

    for doc in cursor:
        info = doc.get("data", {}).get("info", {}) or {}
        ts = info.get("gameStartTimestamp", 0)
        friends = doc.get("friends_present", [])

        for pid in friends:

            if pid not in user_stats:
                user_stats[pid] = {"records": []}

            for p in info.get("participants", []):
                if p.get("puuid") == pid:
                    win = 1 if p.get("win") else 0
                    user_stats[pid]["records"].append((ts, win))

    # Mapeo a personas
    collection_name = get_users_index_collection(pool_id)
    users_index = {
        d["puuids"]: d["persona"]
        for d in db[collection_name].aggregate([
            {"$unwind": "$puuids"},
            {"$project": {"puuids": 1, "persona": 1}}
        ])
    }

    streaks = {}

    for pid, stats in user_stats.items():
        persona = users_index.get(pid)
        if not persona:
            continue

        records = sorted(stats["records"], key=lambda x: x[0])
        win_list = [w for _, w in records]

        max_w, max_l, cur = compute_streaks(win_list)

        streaks[persona] = {
            "max_win_streak": max_w,
            "max_lose_streak": max_l,
            "current_streak": cur
        }

    # JSON final estándar
    output = {
        "source_L1": l1_name,
        "generated_at": now_str(),
        "start_date": start_date if start_date else None,
        "end_date": end_date if end_date else None,
        "streaks": streaks
    }

    # Nombre del archivo
    if start_date and end_date:
        filename = f"metrics_04_win_lose_streak_{start_date}_to_{end_date}.json"
    else:
        filename = "metrics_04_win_lose_streak.json"

    dataset_folder.mkdir(parents=True, exist_ok=True)
    json_path = dataset_folder / filename

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE)
    parser.add_argument("--min", type=int, default=DEFAULT_MIN)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--pool", type=str, default=None)
    args = parser.parse_args()

    queue = args.queue
    min_friends = args.min
    start_date = args.start
    end_date = args.end

    l1_name = auto_select_l1(queue, min_friends, args.pool)
    if not l1_name:
        print("[04] ERROR: no L1 collection found.")
        return

    pool_id = extract_pool_from_l1(l1_name)

    # Donde guardar
    if start_date and end_date:
        dataset_folder = RUNTIME_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    else:
        dataset_folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"

    compute_metrics(l1_name, dataset_folder, pool_id, start_date, end_date)
    print("[04] Ended")


if __name__ == "__main__":
    main()
