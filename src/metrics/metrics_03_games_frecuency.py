import os
import json
import datetime
import sys
from collections import defaultdict
from pathlib import Path
import argparse
from dotenv import load_dotenv
from pymongo import MongoClient

sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

BASE_DIR = Path(__file__).resolve().parents[2]
RESULTS_DIR = BASE_DIR / "data" / "results"
RUNTIME_DIR = BASE_DIR / "data" / "runtime"

DEFAULT_QUEUE = 440
DEFAULT_MIN = 5
DEFAULT_POOL = "ac89fa8d"


def to_date(ts_ms):
    if not ts_ms:
        return None
    dt = datetime.datetime.utcfromtimestamp(ts_ms / 1000)
    return dt.date().isoformat()


def ensure_date_range(daily_dict, min_date, max_date):
    start = datetime.date.fromisoformat(min_date)
    end = datetime.date.fromisoformat(max_date)

    out = []
    cur = start
    while cur <= end:
        ds = cur.isoformat()
        out.append({"date": ds, "games": daily_dict.get(ds, 0)})
        cur += datetime.timedelta(days=1)
    return out


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_users_index():
    coll = db["L0_users_index"]

    users_by_persona = {}
    puuid_to_persona = {}
    personas_index = []

    cursor = coll.find({}, {"_id": 0, "persona": 1, "riotIds": 1, "puuids": 1})

    for doc in cursor:
        persona = doc.get("persona")
        if not persona:
            continue

        personas_index.append(persona)
        riot_ids = doc.get("riotIds", [])
        puuids = doc.get("puuids", [])

        users_by_persona[persona] = {
            "riotIds": riot_ids,
            "puuids": puuids,
        }

        for pid in puuids:
            puuid_to_persona[pid] = persona

    return users_by_persona, puuid_to_persona, personas_index


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

    if start_date and end_date:
        dataset_folder = RUNTIME_DIR / f"pool_{DEFAULT_POOL}" / f"q{queue}" / f"min{min_friends}"
    else:
        dataset_folder = RESULTS_DIR / f"pool_{DEFAULT_POOL}" / f"q{queue}" / f"min{min_friends}"

    dataset_folder.mkdir(parents=True, exist_ok=True)
    print(f"[03] Running for: {dataset_folder}")

    users_by_persona, puuid_to_persona, personas_index = load_users_index()

    l1_collections = [
        name for name in db.list_collection_names()
        if name.startswith(f"L1_q{queue}") 
        and f"min{min_friends}" in name 
        and f"pool_{DEFAULT_POOL}" in name
    ]

    if not l1_collections:
        print(f"[ERROR] No L1 collections for queue={queue} min={min_friends}")
        return

    global_days = defaultdict(int)
    player_days = defaultdict(lambda: defaultdict(int))

    for persona in personas_index:
        _ = player_days[persona]

    min_date = None
    max_date = None

    # Convert date filters to timestamps
    if start_date and end_date:
        start_ts = int(datetime.datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_ts = int(datetime.datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59).timestamp() * 1000)
    else:
        start_ts = None
        end_ts = None

    for coll_name in l1_collections:
        coll = db[coll_name]
        cursor = coll.find({}, {"data.info": 1, "friends_present": 1})

        for doc in cursor:
            info = doc.get("data", {}).get("info", {}) or {}

            game_end = info.get("gameEndTimestamp")
            game_start = info.get("gameStartTimestamp")
            game_creation = info.get("gameCreation")

            ts = game_end or game_start or game_creation
            date = to_date(ts)

            if date is None:
                continue

            # temporal filter
            if start_ts and end_ts:
                if ts < start_ts or ts > end_ts:
                    continue

            if min_date is None or date < min_date:
                min_date = date
            if max_date is None or date > max_date:
                max_date = date

            global_days[date] += 1

            friends_present = doc.get("friends_present", []) or []
            personas_en_partida = set()

            for pid in friends_present:
                persona = puuid_to_persona.get(pid)
                if persona:
                    personas_en_partida.add(persona)

            for persona in personas_en_partida:
                player_days[persona][date] += 1

    if min_date is None:
        print("[03] No matches in this window")
        return

    today_str = datetime.date.today().isoformat()
    if today_str > max_date:
        max_date = today_str

    global_series = ensure_date_range(global_days, min_date, max_date)

    players_out = []
    all_personas = set(personas_index) | set(player_days.keys())

    for persona in sorted(all_personas):
        by_day = player_days.get(persona, {})
        filled = ensure_date_range(by_day, min_date, max_date)
        total = sum(x["games"] for x in filled)

        user_info = users_by_persona.get(persona, {})
        riot_ids = user_info.get("riotIds", [])
        puuids = user_info.get("puuids", [])

        players_out.append(
            {
                "persona": persona,
                "riotIds": riot_ids,
                "puuids": puuids,
                "total_games": total,
                "games": filled,
            }
        )

    if start_date and end_date:
        filename = f"metrics_03_games_frecuency_{start_date}_to_{end_date}.json"
    else:
        filename = "metrics_03_games_frecuency.json"

    output_file = dataset_folder / filename

    save_json(output_file, {
        "start_date": start_date if start_date else None,
        "end_date": end_date if end_date else None,
        "global_frequency": global_series,
        "players_frequency": players_out,
    })

    print("[03] Done")


if __name__ == "__main__":
    main()
