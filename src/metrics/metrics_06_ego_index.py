# metrics_06_ego_index.py

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
DB_NAME = os.getenv("MONGO_DB", "lol_data")
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


# ================================
# L1 helpers
# ================================

def auto_select_l1(queue, min_friends):
    prefix = f"L1_q{queue}_min{min_friends}_"
    candidates = [c for c in db.list_collection_names() if c.startswith(prefix)]
    if not candidates:
        return None
    candidates.sort()
    return candidates[-1]


def extract_pool_from_l1(l1_name):
    # L1_q440_min5_pool_ab12cd34 -> pool_ab12cd34
    return "pool_" + l1_name.split("_pool_", 1)[1]


# ================================
# Logic helpers
# ================================

def get_sorted_matches_for_puuid(coll_matches, puuid, start_ts=None, end_ts=None):
    query = {"friends_present": puuid}

    if start_ts is not None and end_ts is not None:
        query["data.info.gameStartTimestamp"] = {"$gte": start_ts, "$lte": end_ts}

    cursor = coll_matches.find(
        query,
        {
            "data.info.participants": 1,
            "data.info.gameStartTimestamp": 1,
            "data.info.gameDuration": 1,
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


def extract_player(info, puuid):
    for p in info.get("participants", []):
        if p.get("puuid") == puuid:
            return p
    return None


def safe_norm(value, mean_value):
    if mean_value and mean_value > 0:
        return value / mean_value
    return 0.0


# ================================
# Main compute
# ================================

def compute_metrics(l1_name, dataset_folder, start_date=None, end_date=None):
    start_ts = end_ts = None
    if start_date and end_date:
        start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59
        ).timestamp() * 1000)

    coll_matches = db[l1_name]
    coll_users = db["L0_users_index"]

    users = list(coll_users.find({}, {"persona": 1, "puuids": 1}))
    log(f"Usuarios detectados: {len(users)}")

    persona_stats = {}

    for u in users:
        persona = u["persona"]
        puuids = u.get("puuids", [])

        total_kills = total_deaths = total_assists = 0.0
        total_gold = total_dmg_dealt = total_vision = 0.0
        match_count = 0
        lost_by_surrender = 0

        for puuid in puuids:
            matches = get_sorted_matches_for_puuid(coll_matches, puuid, start_ts, end_ts)

            for _, info, match_id in matches:
                p = extract_player(info, puuid)
                if p is None:
                    continue

                kills = p.get("kills", 0)
                deaths = p.get("deaths", 0)
                assists = p.get("assists", 0)

                gold = p.get("goldEarned", 0)
                dmg_dealt = p.get("totalDamageDealtToChampions", 0)
                vision = p.get("visionScore", 0)

                ended_by_surrender_flag = (
                    p.get("gameEndedInSurrender", False)
                    or p.get("gameEndedInEarlySurrender", False)
                )

                duration = info.get("gameDuration", 0)
                if duration and duration < 900:
                    ended_by_surrender_flag = True

                win = p.get("win", False)

                if ended_by_surrender_flag and not win:
                    lost_by_surrender += 1

                match_count += 1
                total_kills += kills
                total_deaths += deaths
                total_assists += assists
                total_gold += gold
                total_dmg_dealt += dmg_dealt
                total_vision += vision

        if match_count > 0:
            avg_kills = total_kills / match_count
            avg_deaths = total_deaths / match_count
            avg_assists = total_assists / match_count
            avg_gold = total_gold / match_count
            avg_damage_dealt = total_dmg_dealt / match_count
            avg_vision = total_vision / match_count
            lost_rate = lost_by_surrender / match_count
        else:
            avg_kills = 0.0
            avg_deaths = 0.0
            avg_assists = 0.0
            avg_gold = 0.0
            avg_damage_dealt = 0.0
            avg_vision = 0.0
            lost_rate = 0.0

        persona_stats[persona] = {
            "match_count": match_count,
            "avg_kills": avg_kills,
            "avg_deaths": avg_deaths,
            "avg_assists": avg_assists,
            "avg_gold": avg_gold,
            "avg_damage_dealt": avg_damage_dealt,
            "avg_vision_score": avg_vision,
            "lost_by_surrender": lost_by_surrender,
            "lost_surrender_rate": lost_rate,
        }

        log(f"{persona} procesado")

    valid_stats = [s for s in persona_stats.values() if s["match_count"] > 0]

    if valid_stats:
        mean_kills = sum(s["avg_kills"] for s in valid_stats) / len(valid_stats)
        mean_assists = sum(s["avg_assists"] for s in valid_stats) / len(valid_stats)
        mean_damage_dealt = sum(s["avg_damage_dealt"] for s in valid_stats) / len(valid_stats)
        mean_gold = sum(s["avg_gold"] for s in valid_stats) / len(valid_stats)
        mean_vision = sum(s["avg_vision_score"] for s in valid_stats) / len(valid_stats)
        mean_lost_rate = sum(s["lost_surrender_rate"] for s in valid_stats) / len(valid_stats)
    else:
        mean_kills = mean_assists = mean_damage_dealt = 0.0
        mean_gold = mean_vision = mean_lost_rate = 0.0

    results = {}

    for persona, s in persona_stats.items():
        if s["match_count"] == 0:
            ego_index = 0.0
            selfish_score = 0.0
            teamplay_score = 0.0
            tilt_score = 0.0
        else:
            kills_norm = safe_norm(s["avg_kills"], mean_kills)
            dmg_norm = safe_norm(s["avg_damage_dealt"], mean_damage_dealt)
            gold_norm = safe_norm(s["avg_gold"], mean_gold)
            assists_norm = safe_norm(s["avg_assists"], mean_assists)
            vision_norm = safe_norm(s["avg_vision_score"], mean_vision)
            lost_rate_norm = safe_norm(s["lost_surrender_rate"], mean_lost_rate)

            selfish_score = 0.4 * kills_norm + 0.3 * dmg_norm + 0.3 * gold_norm
            teamplay_score = 0.5 * assists_norm + 0.5 * vision_norm
            tilt_score = lost_rate_norm
            ego_index = selfish_score - teamplay_score + 0.2 * tilt_score

        results[persona] = {
            "ego_index": ego_index,
            "selfish_score": selfish_score,
            "teamplay_score": teamplay_score,
            "tilt_score": tilt_score,
            "match_count": s["match_count"],
            "avg_kills": s["avg_kills"],
            "avg_deaths": s["avg_deaths"],
            "avg_assists": s["avg_assists"],
            "avg_gold": s["avg_gold"],
            "avg_damage_dealt": s["avg_damage_dealt"],
            "avg_vision_score": s["avg_vision_score"],
            "lost_by_surrender": s["lost_by_surrender"],
            "lost_surrender_rate": s["lost_surrender_rate"],
        }

    if start_date and end_date:
        filename = f"metrics_06_ego_index_{start_date}_to_{end_date}.json"
    else:
        filename = "metrics_06_ego_index.json"

    out_path = dataset_folder / filename

    # Estructura de salida estandarizada
    output_data = {
        "source_L1": l1_name,
        "generated_at": now_utc(),
        "start_date": start_date,
        "end_date": end_date,
        "ego": results
    }

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)

    log(f"Guardado en {out_path}")
    log("Proceso completado.")


# ================================
# CLI entry
# ================================

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

    print("[06] Starting ... using collection: L1_q" + str(queue) + "_min" + str(min_friends))
    
    l1_name = auto_select_l1(queue, min_friends)
    if not l1_name:
        return

    pool_id = extract_pool_from_l1(l1_name)

    if start_date and end_date:
        dataset_folder = RUNTIME_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    else:
        dataset_folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"

    dataset_folder.mkdir(parents=True, exist_ok=True)

    compute_metrics(l1_name, dataset_folder, start_date, end_date)
    
    print("[06] Ended")


if __name__ == "__main__":
    run()
