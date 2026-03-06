#!/usr/bin/env python3
import sys
from pathlib import Path

# Rutas
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from utils.db import get_mongo_client
from utils.config import MONGO_DB, COLLECTION_RAW_MATCHES, COLLECTION_USERS_INDEX, QUEUE_FLEX

def main():
    print("="*50)
    print("DIAGNÓSTICO: CONTEO DE PARTIDAS POR AMIGOS")
    print(f"Pool: Normal ({COLLECTION_USERS_INDEX})")
    print(f"Queue: Flex ({QUEUE_FLEX})")
    print("="*50)

    with get_mongo_client() as client:
        db = client[MONGO_DB]
        
        # 1. Obtener todos los PUUIDs de amigos (Pool Normal)
        print("[1] Cargando PUUIDs de amigos...")
        friend_puuids = set()
        cursor_users = db[COLLECTION_USERS_INDEX].find({}, {"puuids": 1})
        for doc in cursor_users:
            friend_puuids.update(doc.get("puuids", []))
        
        print(f"    Total PUUIDs conocidos: {len(friend_puuids)}")

        # 2. Consultar partidas de Flex
        print("[2] Consultando partidas de Flex en Mongo RAW...")
        query = {"data.info.queueId": QUEUE_FLEX}
        cursor = db[COLLECTION_RAW_MATCHES].find(query, {"data.metadata.participants": 1})
        
        total_flex = 0
        exact_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
        
        for doc in cursor:
            total_flex += 1
            participants = doc.get("data", {}).get("metadata", {}).get("participants", [])
            friends_in_game = sum(1 for p in participants if p in friend_puuids)
            
            if friends_in_game > 0:
                if friends_in_game > 5: friends_in_game = 5 # Por si acaso, aunque en Flex el max es 5
                exact_counts[friends_in_game] = exact_counts.get(friends_in_game, 0) + 1

        print(f"    Total partidas de Flex encontradas: {total_flex}")
        
        # 3. Mostrar Resumen
        print("\nRESUMEN: Partidas por número EXACTO de amigos")
        print("-" * 50)
        for i in range(1, 6):
            print(f"Exactamente {i} amigos: {exact_counts[i]:>6} partidas")
        
        print("\nRESUMEN: Partidas por número MÍNIMO de amigos (Acumulado)")
        print("-" * 50)
        cumulative = 0
        for i in range(5, 0, -1):
            cumulative += exact_counts[i]
            print(f"Mínimo {i} amigos (>= {i}): {cumulative:>6} partidas")
        
        print("-" * 50)
        print(f"NOTA: Si en el Dashboard con min=5 ves {cumulative if i==5 else exact_counts[5]} partidas, y al bajar a min=4 ves {cumulative if i==4 else cumulative}, es correcto.")
        print("Esto es porque 'min=4' incluye TODAS las de 4 amigos MÁS TODAS las de 5 amigos.")

if __name__ == "__main__":
    main()
