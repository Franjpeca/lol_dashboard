# metrics_07_troll_index.py

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
DB_NAME = os.getenv("MONGO_DB")
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

RESULTS_ROOT = Path("data/results")
RUNTIME_ROOT = Path("data/runtime")


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
# Logic helpers
# ============================================================

def detect_afk(p):
    if p.get("timeSpentDead", 0) > 900:
        return True
    if p.get("takedowns", 0) == 0 and p.get("totalMinionsKilled", 0) < 10 and p.get("timePlayed", 0) > 600:
        return True
    return False


def extract_player(info, puuid):
    for p in info.get("participants", []):
        if p.get("puuid") == puuid:
            return p
    return None


def get_sorted_matches_for_puuid(coll_matches, puuid, start_ts, end_ts):
    match_filter = {"friends_present": puuid}

    # ventana temporal opcional
    if start_ts is not None and end_ts is not None:
        match_filter["data.info.gameStartTimestamp"] = {"$gte": start_ts, "$lte": end_ts}

    cursor = coll_matches.find(
        match_filter,
        {
            "data.info.participants": 1,
            "data.info.teams": 1,
            "data.info.gameDuration": 1,
            "data.info.gameStartTimestamp": 1,
            "data.metadata.matchId": 1,
        }
    )

    matches = []
    for doc in cursor:
        info = doc["data"]["info"]
        start = info.get("gameStartTimestamp", 0)
        match_id = doc["data"]["metadata"]["matchId"]
        matches.append((start, info, match_id))

    matches.sort(key=lambda x: x[0])
    return matches


# ============================================================
# MAIN compute
# ============================================================

def compute_troll_index(l1_name, dataset_folder, pool_id: str, start_date, end_date):
    coll_matches = db[l1_name]
    collection_name = get_users_index_collection(pool_id)
    coll_users = db[collection_name]

    # convertir fechas a timestamps
    if start_date and end_date:
        start_ts = date_to_timestamp_ms(start_date, end_of_day=False)
        end_ts = int(datetime.strptime(end_date + " 23:59:59", "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
    else:
        start_ts = None
        end_ts = None

    users = list(coll_users.find({}, {"persona": 1, "puuids": 1}))
    log(f"Usuarios detectados: {len(users)}")

    troll_results = {}

    for u in users:
        persona = u["persona"]
        puuids = u.get("puuids", [])

        total_matches = 0
        early_own = 0
        early_enemy = 0
        afk_own = 0
        afk_enemy = 0

        for puuid in puuids:
            matches = get_sorted_matches_for_puuid(coll_matches, puuid, start_ts, end_ts)

            for _, info, match_id in matches:
                participants = info.get("participants")
                teams = info.get("teams")
                duration = info.get("gameDuration", 0)

                if not participants or not teams or len(participants) != 10 or len(teams) != 2:
                    continue

                player = extract_player(info, puuid)
                if player is None:
                    continue

                total_matches += 1
                team_id = player["teamId"]

                own = [p for p in participants if p["teamId"] == team_id]
                enemy = [p for p in participants if p["teamId"] != team_id]

                # early surrender propio
                if any(p.get("teamEarlySurrendered", False) for p in own):
                    early_own += 1

                # early surrender enemigo
                if any(p.get("teamEarlySurrendered", False) for p in enemy):
                    early_enemy += 1

                # afk propio
                if any(detect_afk(p) for p in own):
                    afk_own += 1

                # afk enemigo
                if any(detect_afk(p) for p in enemy):
                    afk_enemy += 1

        # guardar
        if total_matches == 0:
            troll_results[persona] = {
                "total_matches": 0,
                "early_surrender_own": 0,
                "early_surrender_enemy": 0,
                "afk_own": 0,
                "afk_enemy": 0,
                "pct_early_surrender_own": 0,
                "pct_early_surrender_enemy": 0,
                "pct_afk_own": 0,
                "pct_afk_enemy": 0,
            }
        else:
            troll_results[persona] = {
                "total_matches": total_matches,
                "early_surrender_own": early_own,
                "early_surrender_enemy": early_enemy,
                "afk_own": afk_own,
                "afk_enemy": afk_enemy,
                "pct_early_surrender_own": early_own / total_matches,
                "pct_early_surrender_enemy": early_enemy / total_matches,
                "pct_afk_own": afk_own / total_matches,
                "pct_afk_enemy": afk_enemy / total_matches,
            }

        log(f"{persona} procesado")

    # salida con el formato estandarizado
    out_file = dataset_folder / (
        f"metrics_07_troll_index_{start_date}_to_{end_date}.json"
        if start_date and end_date else "metrics_07_troll_index.json"
    )

    json_data = {
        "source_L1": l1_name,
        "generated_at": now_utc(),
        "start_date": start_date,
        "end_date": end_date,
        "troll": troll_results
    }

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with out_file.open("w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)

    log(f"Guardado en {out_file}")
    log("Proceso completado.")


# ============================================================
# CLI entry
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

    print(f"[07] Starting ... queue={queue} min={min_friends}")

    l1_name = auto_select_l1(queue, min_friends, args.pool)
    if not l1_name:
        print(f"[07] No L1 collection found for q{queue} min{min_friends} pool={args.pool}")
        return

    pool_id = extract_pool_from_l1(l1_name)

    # según ventana: results o runtime
    if start and end:
        folder = RUNTIME_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    else:
        folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"

    folder.mkdir(parents=True, exist_ok=True)

    compute_troll_index(l1_name, folder, pool_id, start, end)

    print("[07] Ended")


if __name__ == "__main__":
    main()
