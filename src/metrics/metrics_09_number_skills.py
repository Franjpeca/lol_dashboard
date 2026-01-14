# metrics_09_number_skills.py

import os
import json
import argparse
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from date_utils import date_to_timestamp_ms
from dotenv import load_dotenv
from pymongo import MongoClient

sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

RESULTS_ROOT = Path("data/results")
RUNTIME_ROOT = Path("data/runtime")


def run():
    main()


def now_utc():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{now_utc()}] {msg}", flush=True)



# ============================================================
# L1 helpers
# ============================================================

def auto_select_l1(queue, min_friends, pool_id=None):
    prefix = f"L1_q{queue}_min{min_friends}_"
    candidates = [c for c in db.list_collection_names() if c.startswith(prefix)]

    if pool_id:
        pool_tag = f"pool_{pool_id}"
        candidates = [c for c in candidates if pool_tag in c]

    if not candidates:
        return None
    candidates.sort()
    return candidates[-1]



def get_users_index_collection(pool_id: str):
    """Returns the correct L0_users_index collection name based on pool_id."""
    return "L0_users_index"


def extract_pool_from_l1(l1_name):
    return "pool_" + l1_name.split("_pool_", 1)[1]


# ============================================================
# MAIN compute
# ============================================================

def compute_metrics(l1_name, dataset_folder, pool_id: str, start_date=None, end_date=None):
    coll_matches = db[l1_name]
    collection_name = get_users_index_collection(pool_id)
    coll_users = db[collection_name]

    # --- Filtro temporal opcional ---
    if start_date and end_date:
        ts_start = date_to_timestamp_ms(start_date, end_of_day=False)
        ts_end = date_to_timestamp_ms(end_date, end_of_day=True)
        match_query = {
            "data.info.gameStartTimestamp": {"$gte": ts_start, "$lte": ts_end}
        }
    else:
        match_query = {}

    users = list(coll_users.find({}, {"persona": 1, "puuids": 1}))

    stats = defaultdict(lambda: {"Q": [], "W": [], "E": [], "R": []})

    for u in users:
        persona = u["persona"]
        puuids = u.get("puuids", [])

        for puuid in puuids:
            cursor = coll_matches.find(
                {"friends_present": puuid, **match_query},
                {"data.info.participants": 1}
            )

            for doc in cursor:
                participants = (
                    doc.get("data", {})
                       .get("info", {})
                       .get("participants", [])
                )

                for p in participants:
                    if p.get("puuid") == puuid:
                        stats[persona]["Q"].append(p.get("spell1Casts", 0))
                        stats[persona]["W"].append(p.get("spell2Casts", 0))
                        stats[persona]["E"].append(p.get("spell3Casts", 0))
                        stats[persona]["R"].append(p.get("spell4Casts", 0))
                        break

    # --- cálculo final ---
    result = {}

    for persona, values in stats.items():
        q = values["Q"]
        w = values["W"]
        e = values["E"]
        r = values["R"]

        result[persona] = {
            "avg_Q": sum(q) / len(q) if q else 0,
            "avg_W": sum(w) / len(w) if w else 0,
            "avg_E": sum(e) / len(e) if e else 0,
            "avg_R": sum(r) / len(r) if r else 0,
            "max_Q": max(q) if q else 0,
            "max_W": max(w) if w else 0,
            "max_E": max(e) if e else 0,
            "max_R": max(r) if r else 0,
            "matches": len(q)
        }

    out_file = dataset_folder / (
        f"metrics_09_number_skills_{start_date}_to_{end_date}.json"
        if start_date and end_date else "metrics_09_number_skills.json"
    )

    out_file.parent.mkdir(parents=True, exist_ok=True)

    json_data = {
        "source_L1": l1_name,
        "generated_at": now_utc(),
        "start_date": start_date,
        "end_date": end_date,
        "skills": result
    }

    with out_file.open("w", encoding="utf-8") as f:
        json.dump(json_data, f, indent=2, ensure_ascii=False)

    log(f"Guardado en {out_file}")
    log("Proceso completado.")



# ============================================================
# CLI ENTRY
# ============================================================

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
    start = args.start
    end = args.end

    print(f"[09] Starting ... using queue={queue} min={min_friends}")

    l1_name = auto_select_l1(queue, min_friends, args.pool)
    if not l1_name:
        print("[09] ERROR: No L1 collection found")
        return

    pool_id = extract_pool_from_l1(l1_name)

    if start and end:
        dataset_folder = RUNTIME_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    else:
        dataset_folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"

    dataset_folder.mkdir(parents=True, exist_ok=True)

    compute_metrics(l1_name, dataset_folder, pool_id, start, end)

    print("[09] Ended")


if __name__ == "__main__":
    run()
