import os
import datetime
import hashlib
import argparse
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

# ============================
# CONFIG
# ============================
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
SRC_COLLECTION = os.getenv("MONGO_COLLECTION_RAW_MATCHES", "L0_all_raw_matches")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
coll_src = db[SRC_COLLECTION]
coll_users = db["L0_users_index"]


# ============================
# UTILS
# ============================
def now_utc():
    return datetime.datetime.now(datetime.UTC)


def build_pool_version(puuids: list[str]) -> str:
    base = ",".join(sorted(puuids))
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]
    return f"pool_{h}"


# ============================
# MAIN
# ============================
def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=int(os.getenv("QUEUE_FLEX", "440")))
    parser.add_argument("--min", type=int, default=int(os.getenv("MIN_FRIENDS_IN_MATCH", "5")))
    args = parser.parse_args()

    queue_id = args.queue
    min_friends = args.min

    print(f"[INIT] queue={queue_id} | min_friends={min_friends}")

    # ============================
    # LOAD FRIENDS POOL
    # ============================
    friend_puuids = set()
    persona_por_puuid = {}

    cursor_users = coll_users.find({}, {"puuids": 1})

    for doc in cursor_users:
        puuids = doc.get("puuids", [])
        for p in puuids:
            friend_puuids.add(p)
            persona_por_puuid[p] = doc["_id"]

    pool_version = build_pool_version(list(friend_puuids))

    print(f"[POOL] total_puuids={len(friend_puuids)} | version={pool_version}")

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
    cursor = coll_src.find(
        {"data.info.queueId": queue_id},
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
