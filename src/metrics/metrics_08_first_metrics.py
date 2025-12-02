# metrics_08_first_metrics.py

import os
import json
import argparse
import sys
from pathlib import Path
from datetime import datetime
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

def auto_select_l1(queue, min_friends):
    prefix = f"L1_q{queue}_min{min_friends}_"
    cands = [c for c in db.list_collection_names() if c.startswith(prefix)]
    if not cands:
        return None
    cands.sort()
    return cands[-1]


def extract_pool_from_l1(l1_name):
    return "pool_" + l1_name.split("_pool_", 1)[1]


# ============================================================
# logic helpers
# ============================================================

def get_matches_for_puuid(coll_matches, puuid, ts_start=None, ts_end=None):

    query = {"friends_present": puuid}

    if ts_start is not None and ts_end is not None:
        query["data.info.gameStartTimestamp"] = {"$gte": ts_start, "$lte": ts_end}

    cursor = coll_matches.find(
        query,
        {"data.info.participants": 1, "data.info.gameStartTimestamp": 1}
    )

    return [doc["data"]["info"] for doc in cursor]


def extract_player(info, puuid):
    for p in info.get("participants", []):
        if p.get("puuid") == puuid:
            return p
    return None


def find_first_death_participant(info):
    players = info.get("participants", [])
    if not players:
        return None

    sorted_by_first_death = sorted(
        players,
        key=lambda x: x.get("longestTimeSpentLiving", 999999)
    )
    return sorted_by_first_death[0].get("puuid")


# ============================================================
# main compute
# ============================================================

def compute_metrics(l1_name, dataset_folder, start_date=None, end_date=None):

    ts_start = ts_end = None
    if start_date and end_date:
        ts_start = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        ts_end = int(datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59
        ).timestamp() * 1000)

    coll_matches = db[l1_name]
    coll_users = db["L0_users_index"]

    users = list(coll_users.find({}, {"persona": 1, "puuids": 1}))
    log(f"Usuarios detectados: {len(users)}")

    results = {}

    for u in users:
        persona = u["persona"]
        puuids = u.get("puuids", [])

        fb_kills = 0
        fb_assists = 0
        first_death_count = 0

        early_takedowns = []
        early_gold = []
        early_dmg = []
        early_vision = []
        early_farm = []

        match_count = 0

        for puuid in puuids:
            matches = get_matches_for_puuid(coll_matches, puuid, ts_start, ts_end)

            for info in matches:
                p = extract_player(info, puuid)
                if not p:
                    continue

                match_count += 1

                if p.get("firstBloodKill", False):
                    fb_kills += 1

                if p.get("firstBloodAssist", False):
                    fb_assists += 1

                first_dead = find_first_death_participant(info)
                if first_dead == puuid:
                    first_death_count += 1

                ch = p.get("challenges", {})

                early_takedowns.append(ch.get("takedownsFirstXMinutes", 0))
                early_gold.append(ch.get("goldPerMinute", 0))
                early_dmg.append(ch.get("damagePerMinute", 0))
                early_vision.append(ch.get("visionScorePerMinute", 0))
                early_farm.append(ch.get("laneMinionsFirst10Minutes", 0))

        if match_count == 0:
            results[persona] = {
                "first_blood_kills": 0,
                "first_blood_kills_rate": 0,
                "first_blood_assists": 0,
                "first_blood_assists_rate": 0,
                "first_death_count": 0,
                "first_death_rate": 0,
                "avg_early_takedowns": 0,
                "avg_early_gold": 0,
                "avg_early_damage": 0,
                "avg_early_vision": 0,
                "avg_early_farm": 0,
                "match_count": 0
            }
        else:
            results[persona] = {
                "first_blood_kills": fb_kills,
                "first_blood_kills_rate": fb_kills / match_count,
                "first_blood_assists": fb_assists,
                "first_blood_assists_rate": fb_assists / match_count,
                "first_death_count": first_death_count,
                "first_death_rate": first_death_count / match_count,
                "avg_early_takedowns": sum(early_takedowns) / len(early_takedowns),
                "avg_early_gold": sum(early_gold) / len(early_gold),
                "avg_early_damage": sum(early_dmg) / len(early_dmg),
                "avg_early_vision": sum(early_vision) / len(early_vision),
                "avg_early_farm": sum(early_farm) / len(early_farm),
                "match_count": match_count
            }

        log(f"{persona} procesado.")

    # === JSON estandarizado ===
    out_file = (
        dataset_folder / f"metrics_08_first_metrics_{start_date}_to_{end_date}.json"
        if start_date and end_date else dataset_folder / "metrics_08_first_metrics.json"
    )

    out_data = {
        "source_L1": l1_name,
        "generated_at": now_utc(),
        "start_date": start_date,
        "end_date": end_date,
        "first_metrics": results
    }

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(out_data, f, indent=2, ensure_ascii=False)

    log(f"Guardado en {out_file}")
    log("Proceso completado.")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE)
    parser.add_argument("--min", type=int, default=DEFAULT_MIN)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    args = parser.parse_args()

    queue = args.queue
    min_friends = args.min
    start = args.start
    end = args.end

    print(f"[08] Starting ... using L1_q{queue}_min{min_friends}")

    l1_name = auto_select_l1(queue, min_friends)
    if not l1_name:
        return

    pool_id = extract_pool_from_l1(l1_name)

    dataset_folder = (
        RUNTIME_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
        if start and end else
        RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    )

    compute_metrics(l1_name, dataset_folder, start, end)

    print("[08] Ended")


if __name__ == "__main__":
    run()
