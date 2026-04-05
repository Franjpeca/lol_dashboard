
import os
import hashlib
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "lol_data")

def build_pool_version(personas: list) -> str:
    base = ",".join(sorted(personas))
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]
    return f"pool_{h}"

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

def check_coll(name):
    print(f"\n--- Checking {name} ---")
    personas = set()
    for doc in db[name].find({}, {"persona": 1}):
        p = doc.get("persona")
        if p:
            personas.add(p)
    
    p_list = sorted(list(personas))
    print(f"Personas: {p_list}")
    print(f"Hash: {build_pool_version(p_list)}")

check_coll("L0_users_index")
check_coll("L0_users_index_season")

client.close()
