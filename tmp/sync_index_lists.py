
import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Cargar .env
load_dotenv()

# Construir URI robusta
def get_db():
    user = os.getenv('MONGO_USER')
    password = os.getenv('MONGO_PASS')
    host = os.getenv('MONGO_HOST', 'localhost')
    port = os.getenv('MONGO_PORT', '27017')
    db_name = os.getenv('MONGO_DB', 'lol_data')
    auth_db = os.getenv('MONGO_AUTH_DB', 'admin')
    
    uri = f"mongodb://{user}:{password}@{host}:{port}/{db_name}?authSource={auth_db}"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    return client[db_name]

def sync():
    db = get_db()
    print("--- Sincronizando Índice de Usuarios ---")
    
    users = list(db['L0_users_index'].find())
    print(f"Encontrados {len(users)} jugadores en el índice.")
    
    for user in users:
        persona = user['_id']
        accounts = user.get('accounts', [])
        current_puuids = set(user.get('puuids', []))
        
        # Recolectar todos los PUUIDs de la lista de cuentas
        all_acc_puuids = [acc['puuid'] for acc in accounts if 'puuid' in acc]
        
        # Ver qué falta
        missing = [p for p in all_acc_puuids if p not in current_puuids]
        
        if missing:
            print(f" [+] Sincronizando {len(missing)} PUUIDs nuevos para {persona}")
            db['L0_users_index'].update_one(
                {'_id': persona},
                {'$addToSet': {'puuids': {'$each': missing}}}
            )
        else:
            print(f" [ok] {persona} ya está al día.")

if __name__ == "__main__":
    try:
        sync()
        print("\n✅ Proceso de sincronización completado.")
    except Exception as e:
        print(f"\n❌ Error de conexión: {e}")
        print("Asegúrate de estar ejecutando esto en el mismo entorno que el Dashboard.")
