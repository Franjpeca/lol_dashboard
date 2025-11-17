import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pprint import pprint

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

# Obtenemos un documento de la colección L1
doc = coll.find_one()

if not doc:
    print("No hay documentos en la colección.")
    exit()

# Mostrar la estructura del documento principal (sin los datos repetidos de los jugadores)
print("=== ESTRUCTURA PRINCIPAL DEL DOCUMENTO ===")
# Mostramos solo los campos principales (sin los datos repetitivos de los jugadores)
pprint({
    "_id": doc.get("_id"),
    "queue": doc.get("queue"),
    "min_friends": doc.get("min_friends"),
    "pool_version": doc.get("pool_version"),
    "friends_present": doc.get("friends_present"),
    "personas_present": doc.get("personas_present"),
    "filtered_at": doc.get("filtered_at"),
    "run_id": doc.get("run_id"),
})

# Mostrar el contenido completo de 'data', sin resumir
data = doc.get("data", {})
print("\n=== Contenido Completo de 'data' ===")
pprint({
    "metadata": data.get("metadata", {}),
    "teams": data.get("teams", []),
    "participants": data.get("participants", []),  # Todos los detalles de los jugadores
})
