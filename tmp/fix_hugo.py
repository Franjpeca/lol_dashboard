
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
client = MongoClient(os.getenv('MONGO_URI'))
db = client[os.getenv('MONGO_DB')]

puuid = 'anwmg72zGLnXtcMvkKm12fzKvM574Lr2iO2nsz7uhSs4ivKr2xOBCp6Yzx_QLRloUeoTXS_sRxnUBw'
riot_id = 'iryrell lover#WORDO'

# Actualizamos a Hugo con una operación atómica de MongoDB
db['L0_users_index'].update_one(
    {'_id': 'Hugo'},
    {
        '$addToSet': {
            'puuids': puuid,
            'riotIds': riot_id,
            'accounts': {'riotId': riot_id, 'puuid': puuid}
        },
        '$set': {
            'updated_at': '2026-04-05T20:20:00Z'
        }
    },
    upsert=True
)
print(f"✅ Cuenta '{riot_id}' vinculada correctamente a Hugo en MongoDB.")
