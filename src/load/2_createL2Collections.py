import os
import datetime
import argparse
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


def now_utc():
    return datetime.datetime.now(datetime.UTC)


def auto_select_l1(queue_id: int, min_friends: int):
    expected_prefix = f"L1_q{queue_id}_min{min_friends}_"

    candidates = [
        c for c in db.list_collection_names()
        if c.startswith(expected_prefix)
    ]

    if not candidates:
        print(f"no L1 found for queue={queue_id} min={min_friends}")
        return None

    candidates.sort()  # pick latest by name
    return candidates[-1]


def build_l2_from_l1(l1_name):

    suffix = l1_name.replace("L1_", "")

    coll_src = db[l1_name]
    coll_players = db[f"L2_players_flat_{suffix}"]
    coll_enemies = db[f"L2_enemies_flat_{suffix}"]
    coll_summary = db[f"L2_matches_summary_{suffix}"]

    coll_players.drop()
    coll_enemies.drop()
    coll_summary.drop()

    print(f"[PROCESS] L2 from {l1_name}")

    cursor = coll_src.find(
        {},
        {
            "_id": 1,
            "data": 1,
            "friends_present": 1,
            "personas_present": 1,
            "queue": 1,
            "min_friends": 1,
            "pool_version": 1,
        },
    )

    total_matches = 0
    total_players = 0
    total_enemies = 0

    for doc in cursor:
        total_matches += 1

        match_id = doc["_id"]
        data = doc.get("data", {})
        info = data.get("info", {})
        participants = info.get("participants", [])
        teams = info.get("teams", [])
        duration = info.get("gameDuration")

        queue_id = doc.get("queue")
        min_friends = doc.get("min_friends")
        pool_version = doc.get("pool_version")
        friends_present = doc.get("friends_present", [])

        summary_doc = {
            "_id": match_id,
            "queue": queue_id,
            "min_friends": min_friends,
            "pool_version": pool_version,
            "duration": duration,
            "teams": teams,
            "friends_present": friends_present,
            "personas_present": doc.get("personas_present", []),
            "filtered_at": now_utc(),
        }

        coll_summary.insert_one(summary_doc)

        for p in participants:
            puuid = p.get("puuid")

            base_doc = {
                "_id": f"{match_id}:{puuid}",
                "match_id": match_id,
                "puuid": puuid,
                "championName": p.get("championName"),
                "teamId": p.get("teamId"),
                "win": p.get("win"),
                "kills": p.get("kills"),
                "deaths": p.get("deaths"),
                "assists": p.get("assists"),
                "lane": p.get("lane"),
                "role": p.get("role"),
                "damage": p.get("totalDamageDealtToChampions"),
                "visionScore": p.get("visionScore"),
                "goldEarned": p.get("goldEarned"),
                "duration": duration,
                "queue": queue_id,
                "min_friends": min_friends,
                "pool_version": pool_version,
                "filtered_at": now_utc(),
            }

            if puuid in friends_present:
                coll_players.insert_one(base_doc)
                total_players += 1
            else:
                coll_enemies.insert_one(base_doc)
                total_enemies += 1

    print(f"[DONE] matches={total_matches}, players={total_players}, enemies={total_enemies}")


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--min", type=int, help="min friends filter")
    args = parser.parse_args()

    min_value = args.min if args.min is not None else DEFAULT_MIN

    print(f"[AUTO] selecting L1 for queue={DEFAULT_QUEUE} min={min_value}")
    l1_name = auto_select_l1(DEFAULT_QUEUE, min_value)

    if not l1_name:
        return

    print(f"[AUTO] selected: {l1_name}")

    build_l2_from_l1(l1_name)


if __name__ == "__main__":
    main()
