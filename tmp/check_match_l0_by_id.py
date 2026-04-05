
import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
client = MongoClient(os.getenv('MONGO_URI'))
db = client[os.getenv('MONGO_DB')]

if len(sys.argv) < 2:
    print("Uso: python tmp/check_match_l0_by_id.py MATCH_ID")
    sys.exit(1)

match_id = sys.argv[1]
doc = db['L0_all_raw_matches'].find_one({'_id': match_id})

if doc:
    print(f"✅ Partida {match_id} ENCONTRADA en L0.")
    print(f"Estructura detectada:")
    print(f" - Tiene 'info': {'info' in doc}")
    print(f" - Tiene 'metadata': {'metadata' in doc}")
    print(f" - Tiene 'data' (deprecated): {'data' in doc}")
    print(f" - QueueID: {doc.get('info', {}).get('queueId')}")
    print(f" - Participantes en metadata: {len(doc.get('metadata', {}).get('participants', []))}")
else:
    print(f"❌ Partida {match_id} NO encontrada en L0.")
