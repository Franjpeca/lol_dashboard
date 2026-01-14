"""
Script para Limpiar MongoDB - PROTEGE L0 (Partidas)

Este script elimina colecciones procesadas (L1, L2, L3, índices)
pero NUNCA toca las colecciones L0 que contienen las partidas descargadas.

Uso:
    python clean_mongo.py              # Modo interactivo (pregunta antes de borrar)
    python clean_mongo.py --confirm    # Borra directamente sin preguntar
    python clean_mongo.py --dry-run    # Solo ver qué se borraría
"""

import os
import sys
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")

# Colecciones que NUNCA se borran (CRÍTICAS)
PROTECTED_COLLECTIONS = [
    "L0_matches",           # Partidas descargadas
    "L0_users_index",       # Índice de usuarios
]

# Prefijos de colecciones que se pueden borrar
CLEANABLE_PREFIXES = [
    "L1_",   # Partidas filtradas
    "L2_",   # Índices procesados
    "L3_",   # Métricas calculadas
]


def get_collections_to_clean(db):
    """
    Obtiene lista de colecciones que se pueden borrar.
    
    Returns:
        tuple: (cleanable, protected) listas de nombres de colecciones
    """
    all_collections = db.list_collection_names()
    
    cleanable = []
    protected = []
    
    for coll in all_collections:
        # Proteger colecciones críticas
        if coll in PROTECTED_COLLECTIONS:
            protected.append(coll)
            continue
        
        # Revisar si es una colección procesada (L1, L2, L3)
        if any(coll.startswith(prefix) for prefix in CLEANABLE_PREFIXES):
            cleanable.append(coll)
    
    return cleanable, protected


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Limpiar colecciones MongoDB excepto L0")
    parser.add_argument("--confirm", action="store_true", help="Confirmar borrado sin preguntar")
    parser.add_argument("--dry-run", action="store_true", help="Solo listar, no borrar")
    args = parser.parse_args()
    
    # Conectar a MongoDB
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        print(f"✅ Conectado a MongoDB: {DB_NAME}")
    except Exception as e:
        print(f"❌ Error conectando a MongoDB: {e}")
        sys.exit(1)
    
    # Obtener colecciones
    cleanable, protected = get_collections_to_clean(db)
    
    print("\n" + "="*60)
    print("🔒 COLECCIONES PROTEGIDAS (NO SE BORRARÁN)")
    print("="*60)
    for coll in protected:
        doc_count = db[coll].estimated_document_count()
        print(f"  - {coll} ({doc_count:,} documentos)")
    
    print("\n" + "="*60)
    print("🗑️  COLECCIONES QUE SE PUEDEN BORRAR")
    print("="*60)
    if not cleanable:
        print("  (Ninguna colección para borrar)")
        sys.exit(0)
    
    total_docs = 0
    for coll in cleanable:
        doc_count = db[coll].estimated_document_count()
        total_docs += doc_count
        print(f"  - {coll} ({doc_count:,} documentos)")
    
    print(f"\n📊 TOTAL: {len(cleanable)} colecciones, {total_docs:,} documentos")
    
    # Modo dry-run
    if args.dry_run:
        print("\n🔍 Modo DRY-RUN: No se borró nada")
        sys.exit(0)
    
    # Confirmación
    if not args.confirm:
        print("\n" + "="*60)
        response = input("¿Borrar estas colecciones? (escribe 'SI' para confirmar): ")
        if response.upper() != "SI":
            print("❌ Cancelado por el usuario")
            sys.exit(0)
    
    # Borrar colecciones
    print("\n🗑️  Borrando colecciones...")
    deleted_count = 0
    for coll in cleanable:
        try:
            db.drop_collection(coll)
            deleted_count += 1
            print(f"  ✅ {coll}")
        except Exception as e:
            print(f"  ❌ {coll}: {e}")
    
    print(f"\n✅ Proceso completado: {deleted_count}/{len(cleanable)} colecciones borradas")
    print(f"🔒 Colecciones L0 protegidas: {len(protected)}")


if __name__ == "__main__":
    main()
