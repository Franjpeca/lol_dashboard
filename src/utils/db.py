
from pymongo import MongoClient
from contextlib import contextmanager

# Import configuration constants to avoid duplicating load_dotenv
from utils.config import MONGO_URI, MONGO_DB

@contextmanager
def get_mongo_client(uri: str = MONGO_URI):
    """
    Context manager for providing a MongoDB client.
    Ensures the client connection is properly closed after use.
    
    Usage:
        with get_mongo_client() as client:
            db = client[MONGO_DB]
            # do operations
    """
    client = None
    try:
        client = MongoClient(uri)
        yield client
    finally:
        if client is not None:
            client.close()

def get_mongo_db_direct(uri: str = MONGO_URI, db_name: str = MONGO_DB):
    """
    Returns a direct reference to the database and client.
    NOTE: The caller is responsible for explicitly calling client.close()
    when finished. Prefer `with get_mongo_client()` when possible.
    """
    client = MongoClient(uri)
    return client, client[db_name]

