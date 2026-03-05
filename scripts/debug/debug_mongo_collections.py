import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

def list_collections():
    uri = os.getenv("MONGO_URI")
    db_name = os.getenv("MONGO_DB", "lol_data")
    
    # Force 127.0.0.1 for testing
    if "localhost" in uri:
        uri = uri.replace("localhost", "127.0.0.1")
    
    print(f"Connecting to {uri}...")
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=2000, directConnection=True)

        client.server_info() # Trigger connection check
        db = client[db_name]
        colls = db.list_collection_names()
        print(f"Collections in {db_name}:")
        for c in sorted(colls):
            print(f" - {c}")
        client.close()
    except Exception as e:
        print(f"Error connecting to DB: {e}")

if __name__ == "__main__":
    list_collections()
