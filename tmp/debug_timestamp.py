import sys
from pathlib import Path
sys.path.append('src')

from utils.db import get_mongo_client
from utils.config import MONGO_DB, COLLECTION_RAW_MATCHES

match_id = 'EUW1_7810023967'
with get_mongo_client() as client:
    db = client[MONGO_DB]
    match = db[COLLECTION_RAW_MATCHES].find_one({'_id': match_id})
    if match:
        ts = match['data']['info']['gameStartTimestamp']
        print(f"Match ID: {match_id}")
        print(f"Timestamp: {ts}")
        import datetime
        dt = datetime.datetime.fromtimestamp(ts / 1000, tz=datetime.timezone.utc)
        print(f"Date (UTC): {dt}")
    else:
        print("Match not found in MongoDB")
