import os
import json
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from date_utils import date_to_timestamp_ms

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# USERS_INDEX will be selected dynamically based on pool_id

RESULTS_ROOT = Path("data/results")
RUNTIME_ROOT = Path("data/runtime")


def get_users_index_collection(pool_id: str):
    """Returns the correct L0_users_index collection name based on pool_id."""
    return "L0_users_index"


def load_puuid_to_user_mapping(pool_id: str):
    """Load user mapping from the correct collection based on pool_id."""
    collection_name = get_users_index_collection(pool_id)
    users_index = db[collection_name]
    
    mapping = {}
    cursor = users_index.find({}, {"persona": 1, "puuids": 1})
    for doc in cursor:
        persona = doc.get("persona")
        for puuid in doc.get("puuids", []):
            mapping[puuid] = persona
    return mapping


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


def extract_pool_from_l1(l1_name):
    return "pool_" + l1_name.split("_pool_", 1)[1]


def compute_player_champions(coll_name, puuid_to_user, dataset_folder, start_date=None, end_date=None):
    coll = db[coll_name]

    query = {}
    if start_date and end_date:
        start_ts = date_to_timestamp_ms(start_date, end_of_day=False)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        end_ts = int(end_dt.timestamp() * 1000)
        query["data.info.gameStartTimestamp"] = {"$gte": start_ts, "$lte": end_ts}

    cursor = coll.find(query, {"data.info.participants": 1})

    total_matches = coll.count_documents(query)
    print(f"[13] Found {total_matches} matches to process")

    stats = defaultdict(lambda: defaultdict(lambda: {"games": 0, "wins": 0}))

    for doc in cursor:
        info = doc.get("data", {}).get("info", {})
        participants = info.get("participants", [])
        if not participants:
            continue

        for p in participants:
            puuid = p.get("puuid")
            if not puuid or puuid not in puuid_to_user:
                continue

            persona = puuid_to_user[puuid]
            champ = p.get("championName")
            if not champ:
                continue

            entry = stats[persona][champ]
            entry["games"] += 1
            if p.get("win"):
                entry["wins"] += 1

    # Use repr() to avoid encoding issues with special characters
    try:
        print(f"[DEBUG] Players found in L1 collection: {sorted(stats.keys())}")
    except UnicodeEncodeError:
        print(f"[DEBUG] Players found in L1 collection: {len(stats)} players (names contain special characters)")

    player_champions = {}
    for persona, champs in stats.items():
        rows = []
        for champ_name, s in champs.items():
            games = s["games"]
            wins = s["wins"]
            if games == 0:
                winrate = 0.0
            else:
                winrate = round(100.0 * wins / games, 2)
            rows.append(
                {
                    "champion": champ_name,
                    "games": games,
                    "wins": wins,
                    "winrate": winrate,
                }
            )
        rows.sort(key=lambda r: r["games"], reverse=True)
        player_champions[persona] = rows

    output = {
        "source_L1": coll_name,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "start_date": start_date,
        "end_date": end_date,
        "player_champions": player_champions,
    }

    dataset_folder.mkdir(parents=True, exist_ok=True)
    if start_date and end_date:
        fname = f"metrics_13_player_champions_stats_{start_date}_to_{end_date}.json"
    else:
        fname = "metrics_13_player_champions_stats.json"

    path = dataset_folder / fname
    with path.open("w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    try:
        print(f"[DEBUG] Final players in champions stats: {sorted(player_champions.keys())}")
    except UnicodeEncodeError:
        print(f"[DEBUG] Final players in champions stats: {len(player_champions)} players")
    print(f"[13] Saved player champions stats to {path}")


def main():
    parser = argparse.ArgumentParser(description="Compute champion stats per player")
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE)
    parser.add_argument("--min", type=int, default=DEFAULT_MIN)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--pool", type=str, default=None)
    args = parser.parse_args()

    print(f"[13] Starting player champions stats for q{args.queue} min{args.min}")

    l1_name = auto_select_l1(args.queue, args.min, args.pool)
    if not l1_name:
        print(f"[13] No L1 collection found for q{args.queue} min{args.min}")
        return

    pool_id = extract_pool_from_l1(l1_name)

    if args.start and args.end:
        dataset_folder = RUNTIME_ROOT / pool_id / f"q{args.queue}" / f"min{args.min}"
    else:
        dataset_folder = RESULTS_ROOT / pool_id / f"q{args.queue}" / f"min{args.min}"

    puuid_to_user = load_puuid_to_user_mapping(pool_id)
    compute_player_champions(l1_name, puuid_to_user, dataset_folder, args.start, args.end)

    print("[13] Done")


if __name__ == "__main__":
    main()
