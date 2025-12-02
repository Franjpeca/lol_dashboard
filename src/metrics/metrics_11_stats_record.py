import os
import json
import argparse
import sys
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime

sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

BASE_DIR = Path(__file__).resolve().parents[2]
RESULTS_ROOT = BASE_DIR / "data" / "results"
RUNTIME_ROOT = BASE_DIR / "data" / "runtime"


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


def format_game_duration(seconds):
    m = seconds // 60
    s = seconds % 60
    return f"{m}m {s}s"


def calculate_record_stats(l1_collection, start_dt, end_dt):
    coll = db[l1_collection]

    puuid_to_persona = {}
    for user in db["L0_users_index"].find({}, {"persona": 1, "puuids": 1}):
        persona = user.get("persona", "Unknown")
        for puuid in user.get("puuids", []):
            puuid_to_persona[puuid] = persona

    records = {}

    cursor = coll.find(
        {},
        {
            "data.info.participants": 1,
            "data.info.gameDuration": 1,
            "data.info.gameStartTimestamp": 1,
            "data.info.gameId": 1,
            "friends_present": 1
        }
    )

    for doc in cursor:
        info = doc.get("data", {}).get("info", {})
        gts = info.get("gameStartTimestamp")
        if not within_window(gts, start_dt, end_dt):
            continue

        participants = info.get("participants", [])
        game_duration = info.get("gameDuration", 0)
        game_id = info.get("gameId", "")
        friends = doc.get("friends_present", [])

        for p in participants:
            puuid = p.get("puuid")
            if puuid not in friends:
                continue

            persona = puuid_to_persona.get(puuid, "Unknown")

            if persona not in records:
                records[persona] = {
                    "max_kills": {"value": 0, "game_id": ""},
                    "max_deaths": {"value": 0, "game_id": ""},
                    "max_assists": {"value": 0, "game_id": ""},
                    "max_vision_score": {"value": 0, "game_id": ""},
                    "max_farm": {"value": 0, "game_id": ""},
                    "max_damage_dealt": {"value": 0, "game_id": ""},
                    "max_gold": {"value": 0, "game_id": ""},
                    "longest_game": {"value": "0m 0s", "game_id": ""}
                }

            kills = p.get("kills", 0)
            deaths = p.get("deaths", 0)
            assists = p.get("assists", 0)
            vision = p.get("visionScore", 0)
            farm = p.get("totalMinionsKilled", 0)
            dmg = p.get("totalDamageDealtToChampions", 0)
            gold = p.get("goldEarned", 0)

            if kills > records[persona]["max_kills"]["value"]:
                records[persona]["max_kills"] = {"value": kills, "game_id": game_id}

            if deaths > records[persona]["max_deaths"]["value"]:
                records[persona]["max_deaths"] = {"value": deaths, "game_id": game_id}

            if assists > records[persona]["max_assists"]["value"]:
                records[persona]["max_assists"] = {"value": assists, "game_id": game_id}

            if vision > records[persona]["max_vision_score"]["value"]:
                records[persona]["max_vision_score"] = {"value": vision, "game_id": game_id}

            if farm > records[persona]["max_farm"]["value"]:
                records[persona]["max_farm"] = {"value": farm, "game_id": game_id}

            if dmg > records[persona]["max_damage_dealt"]["value"]:
                records[persona]["max_damage_dealt"] = {"value": dmg, "game_id": game_id}

            if gold > records[persona]["max_gold"]["value"]:
                records[persona]["max_gold"] = {"value": gold, "game_id": game_id}

            current_long = records[persona]["longest_game"]["value"]
            cm = int(current_long.split("m ")[0])
            cs = int(current_long.split("m ")[1].split("s")[0])
            prev_seconds = cm * 60 + cs

            if game_duration > prev_seconds:
                records[persona]["longest_game"] = {
                    "value": format_game_duration(game_duration),
                    "game_id": game_id
                }

    return records


def save_stats(l1_name, start_date, end_date, stats, folder):
    if start_date and end_date:
        fname = f"metrics_11_stats_record_{start_date}_to_{end_date}.json"
    else:
        fname = "metrics_11_stats_record.json"

    out = folder / fname
    out.parent.mkdir(parents=True, exist_ok=True)

    final = {
        "source_L1": l1_name,
        "generated_at": now_str(),
        "start_date": start_date,
        "end_date": end_date,
        "records": stats,
    }

    with out.open("w", encoding="utf-8") as f:
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
    start = args.start
    end = args.end

    start_dt = datetime.strptime(start, "%Y-%m-%d") if start else None
    end_dt = datetime.strptime(end, "%Y-%m-%d") if end else None

    print(f"[11] Starting ... queue={queue} min={min_friends}")

    l1_name = auto_select_l1(queue, min_friends)
    if not l1_name:
        print("[11] No L1 found")
        return

    pool_id = extract_pool_from_l1(l1_name)

    if start and end:
        folder = RUNTIME_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    else:
        folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"

    stats = calculate_record_stats(l1_name, start_dt, end_dt)
    save_stats(l1_name, start, end, stats, folder)

    print("[11] Ended")


if __name__ == "__main__":
    main()
