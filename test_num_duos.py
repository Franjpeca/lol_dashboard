import os
from dotenv import load_dotenv
from pymongo import MongoClient
from collections import defaultdict

# Cargar variables de entorno
load_dotenv()

# Configuración de conexión a MongoDB
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
COLL_NAME = "L1_q440_min5_pool_ac89fa8d"  # Nombre de la colección L1 (ajustar si es necesario)

print(f"Usando colección: {COLL_NAME}")

# Conexión a MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
coll = db[COLL_NAME]

# --- Cargar mapeo de PUUID a Persona ---
puuid_to_user_mapping = {}
users_coll = db["L0_users_index"]
for user_doc in users_coll.find({}, {"persona": 1, "puuids": 1}):
    for puuid in user_doc.get("puuids", []):
        puuid_to_user_mapping[puuid] = user_doc["persona"]


# ================================================
# Contar cuántas veces cada jugador ha ido a la botlane con otro
# ================================================

# Diccionario para almacenar las combinaciones de botlanes
botlane_pairs_count = defaultdict(int)
total_matches_processed = 0

# Iteramos sobre TODOS los documentos (partidas) de la colección
for match in coll.find({}):
    total_matches_processed += 1
    participants = match.get("data", {}).get("info", {}).get("participants", [])
    
    if not participants:
        continue  # Si no hay participantes, saltamos esta partida
    
    # Mapeamos los jugadores por equipo
    team_100 = [p for p in participants if p.get('teamId') == 100]
    team_200 = [p for p in participants if p.get('teamId') == 200]

    for team in [team_100, team_200]:
        # Filtramos los jugadores de la botlane (ADC y Support)
        adcs = [p for p in team if p.get('teamPosition') == 'BOTTOM']
        supports = [p for p in team if p.get('teamPosition') == 'UTILITY']

        # Combinamos ADCs con Supports para contar los dúos
        for adc in adcs:
            for support in supports:
                adc_persona = puuid_to_user_mapping.get(adc['puuid'])
                support_persona = puuid_to_user_mapping.get(support['puuid'])

                # Solo contamos si ambos jugadores pertenecen a nuestro grupo de amigos
                if not adc_persona or not support_persona:
                    continue

                # Creamos una clave única para cada combinación de jugadores (ordenada)
                duo_key = tuple(sorted([adc_persona, support_persona]))

                # Contamos las combinaciones de botlane
                botlane_pairs_count[duo_key] += 1

# Mostrar las combinaciones más frecuentes
print("\n=== Frecuencia de combinaciones de Botlane ===")
print(f"Partidas totales procesadas: {total_matches_processed}")
for duo, count in sorted(botlane_pairs_count.items(), key=lambda x: x[1], reverse=True):
    print(f"{duo[0]} & {duo[1]}: {count} veces")
