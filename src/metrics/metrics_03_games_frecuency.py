import os
import json
import datetime
from date_utils import date_to_timestamp_ms
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


def to_date(ts_ms):
    if not ts_ms:
        return None
    dt = datetime.datetime.fromtimestamp(ts_ms / 1000, datetime.timezone.utc)
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


def get_users_index_collection(pool_id: str):
    """Returns the correct L0_users_index collection name based on pool_id."""
    return "L0_users_index"


def load_users_index(pool_id: str):
    """Load users index from the correct collection based on pool_id."""
    collection_name = get_users_index_collection(pool_id)
    coll = db[collection_name]

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
    parser.add_argument("--pool", type=str, default="ac89fa8d")
    args = parser.parse_args()

    queue = args.queue
    min_friends = args.min
    start_date = args.start
    end_date = args.end
    pool_id = args.pool

    if start_date and end_date:
        dataset_folder = RUNTIME_DIR / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}"
    else:
        dataset_folder = RESULTS_DIR / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}"

    dataset_folder.mkdir(parents=True, exist_ok=True)
    print(f"[03] Running for: {dataset_folder}")

    users_by_persona, puuid_to_persona, personas_index = load_users_index(pool_id)

    prefix = f"L1_q{queue}_min{min_friends}_"
    l1_collections = [
        name for name in db.list_collection_names()
        if name.startswith(prefix) and f"pool_{pool_id}" in name
    ]

    if not l1_collections:
        print(f"[ERROR] No L1 collections for queue={queue} min={min_friends}")
        return

    print(f"[DEBUG] Found {len(l1_collections)} L1 collections:")
    for coll_name in l1_collections:
        print(f"  - {coll_name}")

    global_days = defaultdict(int)
    player_days = defaultdict(lambda: defaultdict(int))

    min_date = None
    max_date = None

    # Convert date filters to timestamps
    if start_date and end_date:
        start_ts = date_to_timestamp_ms(start_date, end_of_day=False)
        end_ts = date_to_timestamp_ms(end_date, end_of_day=True)
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

    # Debug: Show which players were found (handle Unicode errors)
    try:
        print(f"[DEBUG] Players found in matches: {sorted(player_days.keys())}")
    except UnicodeEncodeError:
        print(f"[DEBUG] Players found in matches: {len(player_days)} players")

    if start_date and end_date:
        min_date = start_date
        max_date = end_date
    else:
        if min_date is None:
            print("[03] No matches in this window")
            return
            
        today_str = datetime.date.today().isoformat()
        if today_str > max_date:
            max_date = today_str

    global_series = ensure_date_range(global_days, min_date, max_date)

    players_out = []
    # Only include players who actually have games in this pool
    for persona in sorted(player_days.keys()):
        by_day = player_days.get(persona, {})
        filled = ensure_date_range(by_day, min_date, max_date)
        total = sum(x["games"] for x in filled)

        # Skip players with no games
        if total == 0:
            continue

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

    try:
        print(f"[DEBUG] Final players in output: {[p['persona'] for p in players_out]}")
    except UnicodeEncodeError:
        print(f"[DEBUG] Final players in output: {len(players_out)} players")
    print(f"[DEBUG] Total players with games: {len(players_out)}")

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
