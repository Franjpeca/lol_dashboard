import json
import os
import argparse
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
RESULTS_ROOT = BASE_DIR / "data" / "results"
RUNTIME_ROOT = BASE_DIR / "data" / "runtime"

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def auto_select_l1(queue, min_friends):
    prefix = f"L1_q{queue}_min{min_friends}_"
    cands = [c for c in db.list_collection_names() if c.startswith(prefix)]
    if not cands:
        return None
    cands.sort()
    return cands[-1]


def extract_pool_from_l1(l1_name):
    return "pool_" + l1_name.split("_pool_", 1)[1]


def ts_to_date(ts_ms):
    if not ts_ms:
        return None
    return datetime.utcfromtimestamp(ts_ms / 1000)


def within_window(game_ts_ms, start_dt, end_dt):
    if start_dt is None and end_dt is None:
        return True
    game_dt = ts_to_date(game_ts_ms)
    if start_dt and game_dt < start_dt:
        return False
    if end_dt and game_dt > end_dt:
        return False
    return True


def process_stats(l1_collection, start_dt, end_dt):
    coll = db[l1_collection]

    puuid_to_persona = {}
    for user in db["L0_users_index"].find({}, {"persona": 1, "puuids": 1}):
        persona = user.get("persona", "Unknown")
        for p in user.get("puuids", []):
            puuid_to_persona[p] = persona

    stats = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    games = defaultdict(lambda: defaultdict(int))
    kp_lists = defaultdict(lambda: defaultdict(list))

    cursor = coll.find(
        {},
        {
            "data.info.participants": 1,
            "data.info.gameStartTimestamp": 1,
            "friends_present": 1,
        }
    )

    for doc in cursor:
        info = doc.get("data", {}).get("info", {})
        game_ts = info.get("gameStartTimestamp")
        participants = info.get("participants", [])
        friends = doc.get("friends_present", [])

        if not within_window(game_ts, start_dt, end_dt):
            continue

        team_kills = defaultdict(int)
        for p in participants:
            team = p.get("teamId")
            team_kills[team] += p.get("kills", 0)

        for p in participants:
            puuid = p.get("puuid")
            if puuid not in friends:
                continue

            persona = puuid_to_persona.get(puuid, "Unknown")
            role = p.get("teamPosition", "UNKNOWN")

            games[role][persona] += 1

            stats[role][persona]["total_damage"] += p.get("totalDamageDealt", 0)
            stats[role][persona]["damage_taken"] += p.get("totalDamageTaken", 0)
            stats[role][persona]["gold"] += p.get("goldEarned", 0)
            stats[role][persona]["farm"] += p.get("totalMinionsKilled", 0)
            stats[role][persona]["vision"] += p.get("visionScore", 0)
            stats[role][persona]["turret"] += p.get("damageDealtToBuildings", 0)
            stats[role][persona]["kills"] += p.get("kills", 0)
            stats[role][persona]["deaths"] += p.get("deaths", 0)
            stats[role][persona]["assists"] += p.get("assists", 0)
            stats[role][persona]["wins"] += (1 if p.get("win", False) else 0)

            team_id = p.get("teamId")
            tk = team_kills.get(team_id, 1)
            kp = ((p.get("kills", 0) + p.get("assists", 0)) / tk) * 100 if tk > 0 else 0
            kp_lists[role][persona].append(kp)

    result = {}

    for role in stats:
        result[role] = {}
        for persona in stats[role]:
            g = games[role][persona]
            if g == 0:
                continue

            kp_list = kp_lists[role][persona]
            avg_kp = round(sum(kp_list) / len(kp_list), 2) if kp_list else 0

            s = stats[role][persona]
            result[role][persona] = {
                "games": g,
                "avg_damage": round(s["total_damage"] / g, 2),
                "avg_damage_taken": round(s["damage_taken"] / g, 2),
                "avg_gold": round(s["gold"] / g, 2),
                "avg_farm": round(s["farm"] / g, 2),
                "avg_vision": round(s["vision"] / g, 2),
                "avg_turret_damage": round(s["turret"] / g, 2),
                "avg_kills": round(s["kills"] / g, 2),
                "avg_deaths": round(s["deaths"] / g, 2),
                "avg_assists": round(s["assists"] / g, 2),
                "avg_kill_participation": avg_kp,
                "winrate": round((s["wins"] / g) * 100, 2),
            }

    return result


def save_stats(l1_name, start_date, end_date, stats, dataset_folder):
    if start_date and end_date:
        fname = f"metrics_10_stats_by_rol_{start_date}_to_{end_date}.json"
    else:
        fname = "metrics_10_stats_by_rol.json"

    out_path = dataset_folder / fname
    out_path.parent.mkdir(parents=True, exist_ok=True)

    final = {
        "source_L1": l1_name,
        "generated_at": now_str(),
        "start_date": start_date,
        "end_date": end_date,
        "roles": stats,
    }

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(final, f, ensure_ascii=False, indent=2)


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

    start_dt = datetime.strptime(start_date, "%Y-%m-%d") if start_date else None
    end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else None

    print(f"[10] Starting ... using collection L1_q{queue}_min{min_friends}")

    l1_name = auto_select_l1(queue, min_friends)
    if not l1_name:
        print("[10] No L1 found")
        return

    pool_id = extract_pool_from_l1(l1_name)

    if start_date and end_date:
        dataset_folder = RUNTIME_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    else:
        dataset_folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"

    dataset_folder.mkdir(parents=True, exist_ok=True)

    stats = process_stats(l1_name, start_dt, end_dt)
    save_stats(l1_name, start_date, end_date, stats, dataset_folder)

    print("[10] Ended")


def run():
    main()


if __name__ == "__main__":
    run()
