import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
client = MongoClient(MONGO_URI)
db = client["lol_data"]

def debug_season_data():
    out_lines = []
    out_lines.append("--- DEBUGGING SEASON DATA ---")
    
    # Check L0_users_index_season
    l0_coll = db["L0_users_index_season"]
    count_l0 = l0_coll.count_documents({})
    out_lines.append(f"L0_users_index_season count: {count_l0}")
    
    users = list(l0_coll.find({}, {"persona": 1, "puuids": 1}))
    sample_puuids = []
    if users:
        out_lines.append(f"Sample User: {users[0]['persona']}")
        sample_puuids = users[0].get("puuids", [])
        out_lines.append(f"Sample PUUIDs L0: {sample_puuids}")
    
    # Check L1 collection for season
    # We need to find one
    l1_colls = [c for c in db.list_collection_names() if "L1_q440_min2" in c and "pool_season" in c]
    if not l1_colls:
        out_lines.append("No L1 collection found for season q440 min2")
        with open("debug_result.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(out_lines))
        return
        
    l1_name = l1_colls[0]
    out_lines.append(f"Checking L1 Collection: {l1_name}")
    
    l1_coll = db[l1_name]
    doc = l1_coll.find_one()
    
    if not doc:
        out_lines.append("L1 collection is empty")
        with open("debug_result.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(out_lines))
        return
        
    friends_present = doc.get("friends_present", [])
    out_lines.append(f"Sample Document friends_present in L1: {friends_present}")
    
    # Check matches
    matches = 0
    for fp in friends_present:
        found = False
        for u in users:
            if fp in u.get("puuids", []):
                out_lines.append(f"MATCH! PUUID {fp} belongs to {u['persona']}")
                found = True
                matches += 1
                break
        if not found:
            out_lines.append(f"NO MATCH for PUUID {fp}")
            
    out_lines.append(f"Total matches in sample doc: {matches}/{len(friends_present)}")
    
    with open("debug_result.txt", "w", encoding="utf-8") as f:
        f.write("\n".join(out_lines))


if __name__ == "__main__":
    debug_season_data()
