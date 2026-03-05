import os
import sys
import datetime
import argparse
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient

# Asegurar que src/ esté en el path para importar utils
_FILE_SELF = Path(__file__).resolve()
_SRC_DIR = _FILE_SELF.parents[1]  # src/
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from utils.pool_manager import build_pool_version
from utils.config import MONGO_DB, QUEUE_FLEX, MIN_FRIENDS_IN_MATCH
from utils.db import get_mongo_client

DEFAULT_MIN = MIN_FRIENDS_IN_MATCH
DEFAULT_QUEUE = QUEUE_FLEX


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


# build_pool_version importado desde utils.pool_manager (fuente de verdad única)

def auto_select_l1(queue_id: int, min_friends: int, pool_id: str = None):
    """
    Select L1 collection for the given queue and min_friends.
    If pool_id is provided, use it directly.
    Otherwise, calculate pool from L0_users_index using personas (same as L1).
    """
    # Auto-calculate pool from L0_users_index using PERSONAS (not PUUIDs)
    with get_mongo_client() as client:
        db = client[MONGO_DB]
        
        if pool_id:
            # Use provided pool ID
            l1_name = f"L1_q{queue_id}_min{min_friends}_pool_{pool_id}"
    
            if l1_name not in db.list_collection_names():
                print(f"[WARN] L1 collection {l1_name} does NOT exist.")
                return None
    
            print(f"[INFO] Using specified pool: {pool_id}")
            return l1_name

        coll_users = db["L0_users_index"]
        personas = set()
    
        cursor = coll_users.find({}, {"persona": 1})
        for doc in cursor:
            persona = doc.get("persona")
            if persona:
                personas.add(persona)

        if not personas:
            print("[WARN] No personas found in L0_users_index")
            return None
    
        pool_version = build_pool_version(list(personas))
        l1_name = f"L1_q{queue_id}_min{min_friends}_{pool_version}"
    
        if l1_name not in db.list_collection_names():
            print(f"[WARN] Expected L1 collection {l1_name} does NOT exist.")
            return None

    return l1_name


def build_l2_from_l1(l1_name):

    suffix = l1_name.replace("L1_", "")

    with get_mongo_client() as client:
        db = client[MONGO_DB]
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
    
            # fechas de la partida
            game_start = info.get("gameStartTimestamp")
            game_end = info.get("gameEndTimestamp")
    
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
                "gameStartTimestamp": game_start,
                "gameEndTimestamp": game_end,
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
                    "gameStartTimestamp": game_start,
                    "gameEndTimestamp": game_end,
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
    parser.add_argument("--pool", type=str, default=None, help="pool ID (8 chars del hash). Si no se indica, se auto-calcula desde L0_users_index.")
    parser.add_argument("--users-collection", type=str, default="L0_users_index", help="Users collection (for compatibility)")
    args = parser.parse_args()


    min_value = args.min if args.min is not None else DEFAULT_MIN
    pool_id = args.pool

    print(f"[AUTO] selecting L1 for queue={DEFAULT_QUEUE} min={min_value} pool={pool_id}")
    l1_name = auto_select_l1(DEFAULT_QUEUE, min_value, pool_id)

    if not l1_name:
        return

    print(f"[AUTO] selected: {l1_name}")

    build_l2_from_l1(l1_name)


if __name__ == "__main__":
    main()
