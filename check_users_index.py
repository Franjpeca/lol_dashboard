import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def check_users():
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB", "lol_data")
    client = MongoClient(uri)
    db = client[db_name]
    coll = db["L0_users_index"]
    personas = [doc["persona"] for doc in coll.find({}, {"persona":1})]
    print(f"Personas in L0_users_index: {sorted(personas)}")
    client.close()

if __name__ == "__main__":
    check_users()
