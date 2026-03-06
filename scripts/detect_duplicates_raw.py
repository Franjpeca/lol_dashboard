#!/usr/bin/env python3
import sys
from pathlib import Path

# Rutas
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from utils.db import get_mongo_client
from utils.config import MONGO_DB, COLLECTION_RAW_MATCHES

def main():
    print("="*60)
    print("DIAGNÓSTICO: DETECCIÓN DE DUPLICADOS EN RAW")
    print(f"Colección: {COLLECTION_RAW_MATCHES}")
    print("="*60)

    with get_mongo_client() as client:
        db = client[MONGO_DB]
        coll = db[COLLECTION_RAW_MATCHES]
        
        total_docs = coll.count_documents({})
        print(f"Total documentos en {COLLECTION_RAW_MATCHES}: {total_docs}")
        
        if total_docs == 0:
            print("No hay partidas para analizar.")
            return

        # 1. Duplicados por Match ID de Riot
        print("\n[1] Verificando duplicados por Riot Match ID (metadata)...")
        pipeline = [
            {"$group": {
                "_id": "$data.metadata.matchId",
                "count": {"$sum": 1}
            }},
            {"$match": {"count": {"$gt": 1}}}
        ]
        
        duplicates = list(coll.aggregate(pipeline))
        
        if not duplicates:
            print("    ✅ No se encontraron duplicados por Match ID.")
        else:
            print(f"    ❌ Se encontraron {len(duplicates)} Match IDs duplicados!")
            for d in duplicates[:5]:
                print(f"       - {d['_id']}: {d['count']} ocurrencias")
            if len(duplicates) > 5:
                print("       ...")

        # 2. Consistencia de IDs
        print("\n[2] Verificando consistencia de _id vs matchId...")
        mismatch_query = {
            "$and": [
                {"data.metadata.matchId": {"$exists": True}},
                {"$expr": {"$ne": ["$_id", "$data.metadata.matchId"]}}
            ]
        }
        mismatches = coll.count_documents(mismatch_query)
        if mismatches == 0:
            print("    ✅ Todos los documentos tienen el _id igual al matchId.")
        else:
            print(f"    ⚠️  Hay {mismatches} documentos donde el _id no coincide con el matchId.")

        # 3. Resumen Final
        print("\n[3] Resumen de integridad:")
        unique_match_ids = len(coll.distinct("data.metadata.matchId"))
        print(f"    Partidas Únicas (por metadata.matchId): {unique_match_ids}")
        print(f"    Documentos Totales:                     {total_docs}")
        
        if unique_match_ids == total_docs:
            print("\n✅ CONCLUSIÓN FINAL: Los datos son íntegros. No hay duplicidad.")
        else:
            dupes_count = total_docs - unique_match_ids
            print(f"\n❌ CONCLUSIÓN FINAL: Se han detectado {dupes_count} documentos de más (posibles duplicados).")

if __name__ == "__main__":
    main()
