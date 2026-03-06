#!/usr/bin/env python3
import sys
from pathlib import Path

# Rutas
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from utils.db import get_mongo_client
from utils.config import MONGO_DB, COLLECTION_RAW_MATCHES, COLLECTION_USERS_INDEX, QUEUE_FLEX

def main():
    print("="*60)
    print("VERIFICACIÓN INDEPENDIENTE: MONGODB AGGREGATION")
    print(f"Base de datos: {MONGO_DB}")
    print(f"Colección RAW: {COLLECTION_RAW_MATCHES}")
    print(f"Queue: {QUEUE_FLEX} (Flex)")
    print("="*60)

    with get_mongo_client() as client:
        db = client[MONGO_DB]
        
        # 1. Obtener todos los PUUIDs de amigos de forma limpia
        print("[1] Cargando lista de amigos desde el índice...")
        friend_puuids = []
        cursor_users = db[COLLECTION_USERS_INDEX].find({}, {"puuids": 1})
        for doc in cursor_users:
            friend_puuids.extend(doc.get("puuids", []))
        
        # Eliminar duplicados si los hubiera
        friend_puuids = list(set(friend_puuids))
        print(f"    Total PUUIDs únicos cargados: {len(friend_puuids)}")

        # 2. Pipeline de Agregación (Cálculo directo en MongoDB)
        print("[2] Ejecutando Pipeline de Agregación en MongoDB...")
        
        pipeline = [
            # Filtramos solo partidas de Flex
            {"$match": {"data.info.queueId": QUEUE_FLEX}},
            
            # Calculamos el número de amigos en cada partida
            {"$addFields": {
                "friends_in_game": {
                    "$size": {
                        "$setIntersection": [
                            "$data.metadata.participants", 
                            friend_puuids
                        ]
                    }
                }
            }},
            
            # Filtramos solo las que tienen al menos 1 amigo
            {"$match": {"friends_in_game": {"$gt": 0}}},
            
            # Agrupamos por el conteo de amigos
            {"$group": {
                "_id": "$friends_in_game",
                "total_partidas": {"$sum": 1}
            }},
            
            # Ordenamos por número de amigos
            {"$sort": {"_id": 1}}
        ]
        
        results = list(db[COLLECTION_RAW_MATCHES].aggregate(pipeline))
        
        if not results:
            print("No se encontraron resultados con los criterios especificados.")
            return

        # Formatear resultados
        exact_counts = {i: 0 for i in range(1, 6)}
        for res in results:
            friends_count = res["_id"]
            if friends_count > 5: friends_count = 5 # Capamos a 5 por si hay errores de datos
            exact_counts[friends_count] += res["total_partidas"]

        print("\n>>> RESULTADOS DE AGREGACIÓN PURA (MONGO) <<<")
        print("-" * 60)
        print(f"{'Amigos Exactos':<20} | {'Partidas':<15}")
        print("-" * 60)
        for i in range(1, 6):
            print(f"{i:<20} | {exact_counts[i]:>15}")
        print("-" * 60)

        print("\n>>> TOTALES ACUMULADOS (DASHBOARD) <<<")
        print("-" * 60)
        print(f"{'Mínimo de Amigos':<20} | {'Total Acumulado':<15}")
        print("-" * 60)
        cumulative = 0
        for i in range(5, 0, -1):
            cumulative += exact_counts[i]
            print(f"Mínimo {i} (>= {i}):{i*0:2} | {cumulative:>15}")
        print("-" * 60)

        # 3. Verificación adicional: Total en la base de datos de Flex sin filtros de amigos
        total_flex_raw = db[COLLECTION_RAW_MATCHES].count_documents({"data.info.queueId": QUEUE_FLEX})
        print(f"\nTotal partidas de FLEX en Mongo RAW (con 0, 1, 2... amigos): {total_flex_raw}")

if __name__ == "__main__":
    main()
