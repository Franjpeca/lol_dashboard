import os
import sys
import datetime
import argparse
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

# Asegurar que src/ esté en el path para importar utils
_FILE_SELF = Path(__file__).resolve()
_SRC_DIR = _FILE_SELF.parents[1]  # src/
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from utils.pool_manager import build_pool_version
from utils.config import MONGO_DB, COLLECTION_RAW_MATCHES, QUEUE_FLEX, MIN_FRIENDS_IN_MATCH
from utils.db import get_mongo_client



# ============================
# UTILS
# ============================
def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


# build_pool_version importado desde utils.pool_manager (fuente de verdad única)


# ============================
# MAIN
# ============================
def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=QUEUE_FLEX)
    parser.add_argument("--min", type=int, default=MIN_FRIENDS_IN_MATCH)
    parser.add_argument("--pool", type=str, default=None, help="Pool ID to use (if not provided, auto-calculate from users index)")
    parser.add_argument("--users-collection", type=str, default="L0_users_index", help="Users index collection to read from")
    args = parser.parse_args()

    queue_id = args.queue
    min_friends = args.min
    pool_id_arg = args.pool
    users_collection = args.users_collection

    print(f"[INIT] queue={queue_id} | min_friends={min_friends}")
    print(f"[INIT] users_collection={users_collection}")

    # ============================
    # MAIN DB CONNECTION
    # ============================
    with get_mongo_client() as client:
        db = client[MONGO_DB]
        coll_users = db[users_collection]
        
        friend_puuids = set()
        persona_por_puuid = {}
        personas = set()

        cursor_users = coll_users.find({}, {"persona": 1, "puuids": 1})

        for doc in cursor_users:
            persona = doc.get("persona")
            if persona:
                personas.add(persona)
            
            puuids = doc.get("puuids", [])
            for p in puuids:
                friend_puuids.add(p)
                persona_por_puuid[p] = doc["_id"]

        # Use provided pool ID or calculate from personas
        if pool_id_arg:
            pool_version = f"pool_{pool_id_arg}"
            print(f"[POOL] Using specified pool: {pool_version}")
        else:
            pool_version = build_pool_version(sorted(list(personas)))
            print(f"[POOL] Auto-calculated pool from {len(personas)} personas: {pool_version}")
        
        print(f"[POOL] total_puuids={len(friend_puuids)}")

        # ============================
        # CREATE DEST COLLECTION
        # ============================
        coll_name = f"L1_q{queue_id}_min{min_friends}_{pool_version}"
        coll_dest = db[coll_name]
        coll_dest.drop()

        print(f"[BUILD] creating collection: {coll_name}")

        # ============================
        # FILTER MATCHES
        # ============================
            
        query = {"data.info.queueId": queue_id}
            
        # [SEASON LOGIC] If pool is 'season', enforce start date
        if pool_id_arg == "season":
            # 2026-01-08 00:00:00 UTC = 1767830400000 approx
            # Using exact timestamp for 2026-01-08
            TIMESTAMP_2026_01_08 = 1767830400000
            query["data.info.gameStartTimestamp"] = {"$gte": TIMESTAMP_2026_01_08}
            print(f"[FILTER] Pool 'season' detected. Enforcing gameStartTimestamp >= {TIMESTAMP_2026_01_08} (2026-01-08)")

        coll_src = db[COLLECTION_RAW_MATCHES]
        cursor = coll_src.find(
            query,
            {"_id": 1, "data": 1}
        )

        ops = []
        total_inserted = 0

        for doc in cursor:
            mid = doc["_id"]
            data = doc.get("data", {})
            metadata = data.get("metadata", {})
            participants = metadata.get("participants", [])

            friends_present = [p for p in participants if p in friend_puuids]

            if len(friends_present) >= min_friends:

                personas_present = list({
                    persona_por_puuid[p] for p in friends_present if p in persona_por_puuid
                })

                record = {
                    "_id": mid,
                    "queue": queue_id,
                    "min_friends": min_friends,
                    "pool_version": pool_version,
                    "friends_present": friends_present,
                    "personas_present": personas_present,
                    "filtered_at": now_utc(),
                    "run_id": f"{now_utc().strftime('%Y%m%d_%H%M%S')}",
                    "data": data
                }

                ops.append(UpdateOne({"_id": mid}, {"$set": record}, upsert=True))
                total_inserted += 1

                if len(ops) >= 500:
                    coll_dest.bulk_write(ops, ordered=False)
                    ops = []

        if ops:
            coll_dest.bulk_write(ops, ordered=False)

        print(f"[DONE] inserted={total_inserted}")
        print(f"[CHECK] total_docs={coll_dest.count_documents({})}")

        if total_inserted > 0:
            example = coll_dest.find_one({}, {"_id": 1, "friends_present": 1, "personas_present": 1})
            print(f"[EXAMPLE] {example['_id']} | friends={len(example['friends_present'])} | personas={example['personas_present']}")


if __name__ == "__main__":
    print("[BOOT] starting L1 generator...", flush=True)
    try:
        main()
    except Exception as e:
        import traceback
        print("[FATAL ERROR]", e)
        traceback.print_exc()
