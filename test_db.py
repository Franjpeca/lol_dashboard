import os
import sys
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from pymongo import MongoClient

# Base Dir
BASE_DIR = Path("z:/lol_dashboard")
load_dotenv(dotenv_path=BASE_DIR / ".env", override=True)

print(f"MONGO_URI: {os.getenv('MONGO_URI')}")
print(f"POSTGRES_URI: {os.getenv('POSTGRES_URI')}")

try:
    client = MongoClient(os.getenv('MONGO_URI'), serverSelectionTimeoutMS=2000)
    print(f"Mongo ping: {client.admin.command('ping')}")
except Exception as e:
    print(f"Mongo error: {e}")

try:
    conn = psycopg2.connect(os.getenv('POSTGRES_URI').replace("postgresql+psycopg2://", "postgresql://"))
    print("Postgres connected!")
    conn.close()
except Exception as e:
    print(f"Postgres error: {e}")
