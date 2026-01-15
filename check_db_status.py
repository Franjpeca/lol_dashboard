import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

def check_db():
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB", "lol_data")
    client = MongoClient(uri)
    db = client[db_name]
    colls = db.list_collection_names()
    print(f"Collections in {db_name}:")
    for c in sorted(colls):
        print(f" - {c}")
    client.close()

if __name__ == "__main__":
    check_db()
