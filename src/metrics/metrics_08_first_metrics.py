# metrics_08_first_metrics.py

import os
import json
import argparse
from pathlib import Path
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
# logic helpers
# ============================================================

def get_matches_for_puuid(coll_matches, puuid):
    cursor = coll_matches.find(
        {"friends_present": puuid},
        {"data.info.participants": 1}
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

def compute_metrics(l1_name, dataset_folder):
    coll_matches = db[l1_name]
    coll_users = db["L0_users_index"]

    users = list(coll_users.find({}, {"persona": 1, "puuids": 1}))
    results = {}

    log(f"Usuarios detectados: {len(users)}")

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
            matches = get_matches_for_puuid(coll_matches, puuid)

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

        # sin partidas
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
            continue

        # con partidas
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

    out_path = dataset_folder / "metrics_08_first_metrics.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    log(f"Guardado en {out_path}")
    log("Proceso completado.")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE)
    parser.add_argument("--min", type=int, default=DEFAULT_MIN)
    args = parser.parse_args()

    queue = args.queue
    min_friends = args.min

    print("[08] Starting ... using collection: L1_q" + str(queue) + "_min" + str(min_friends))
    
    l1_name = auto_select_l1(queue, min_friends)
    if not l1_name:
        return

    pool_id = extract_pool_from_l1(l1_name)

    dataset_folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    dataset_folder.mkdir(parents=True, exist_ok=True)

    compute_metrics(l1_name, dataset_folder)
    
    print("[08] Ended")


if __name__ == "__main__":
    run()
