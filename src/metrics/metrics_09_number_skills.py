# metrics_09_number_skills.py

import os
import json
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

RESULTS_ROOT = Path("data/results")


def run():
    main()


def now_utc():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg):
    print(f"[{now_utc()}] {msg}", flush=True)


# ============================================================
# L1 helpers
# ============================================================

def auto_select_l1(queue, min_friends):
    prefix = f"L1_q{queue}_min{min_friends}_"
    candidates = [c for c in db.list_collection_names() if c.startswith(prefix)]
    if not candidates:
        return None
    candidates.sort()
    return candidates[-1]


def extract_pool_from_l1(l1_name):
    return "pool_" + l1_name.split("_pool_", 1)[1]


# ============================================================
# MAIN compute
# ============================================================

def compute_metrics(l1_name, dataset_folder):
    coll_matches = db[l1_name]
    coll_users = db["L0_users_index"]

    users = list(coll_users.find({}, {"persona": 1, "puuids": 1}))

    stats = defaultdict(lambda: {"Q": [], "W": [], "E": [], "R": []})

    for u in users:
        persona = u["persona"]
        puuids = u.get("puuids", [])

        for puuid in puuids:
            cursor = coll_matches.find(
                {"friends_present": puuid},
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

    out_path = dataset_folder / "metrics_09_number_skills.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    log(f"Guardado en {out_path}")
    log("Proceso completado.")


# ============================================================
# CLI ENTRY
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE)
    parser.add_argument("--min", type=int, default=DEFAULT_MIN)
    args = parser.parse_args()

    queue = args.queue
    min_friends = args.min

    print("[09] Starting ... using collection: L1_q" + str(queue) + "_min" + str(min_friends))
    
    l1_name = auto_select_l1(queue, min_friends)
    if not l1_name:
        return

    pool_id = extract_pool_from_l1(l1_name)

    dataset_folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    dataset_folder.mkdir(parents=True, exist_ok=True)

    compute_metrics(l1_name, dataset_folder)
    
    print("[09] Ended")


if __name__ == "__main__":
    run()
