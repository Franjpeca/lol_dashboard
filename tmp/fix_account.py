
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()
client = MongoClient(os.getenv('MONGO_URI'))
db = client[os.getenv('MONGO_DB')]

# PUUID verificado manualmente via API
puuid = 'anwmg72zGLnXtcMvkKm12fzKvM574Lr2iO2nsz7uhSs4ivKr2xOBCp6Yzx_QLRloUeoTXS_sRxnUBw'
riot_id = 'iryrell lover#WORDO'
persona = 'Fran' # Corregido: pertenece a Fran segun mapa_cuentas.json

# Actualizamos a Fran en el índice L0 de MongoDB
# Utilizamos $addToSet para no duplicar si ya existiera parcial
db['L0_users_index'].update_one(
    {'_id': persona},
    {
        '$addToSet': {
            'puuids': puuid,
            'riotIds': riot_id,
            'accounts': {'riotId': riot_id, 'puuid': puuid}
        },
        '$set': {
            'updated_at': '2026-04-05T20:25:00Z'
        }
    },
    upsert=True
)
print(f"✅ Cuenta '{riot_id}' vinculada correctamente a {persona} en MongoDB.")
