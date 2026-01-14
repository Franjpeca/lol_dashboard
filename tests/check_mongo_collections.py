"""
Check MongoDB collections for min3 configuration.
"""
import os
import sys
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")

def check_collections():
    """Check what collections exist for different min_friends values."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    
    all_collections = db.list_collection_names()
    
    print("\n" + "="*70)
    print("MONGODB COLLECTIONS ANALYSIS")
    print("="*70 + "\n")
    
    for min_val in [3, 4, 5]:
        print(f"\n{'='*70}")
        print(f"Collections for min={min_val}")
        print(f"{'='*70}\n")
        
        # L1 collections
        l1_prefix = f"L1_q440_min{min_val}_"
        l1_colls = [c for c in all_collections if c.startswith(l1_prefix)]
        
        print(f"L1 Collections ({len(l1_colls)} found):")
        if l1_colls:
            for coll in sorted(l1_colls):
                count = db[coll].count_documents({})
                print(f"  ✓ {coll} ({count:,} documents)")
        else:
            print(f"  ❌ No L1 collections found")
        
        # L2 collections
        l2_prefix = f"L2_players_flat_q440_min{min_val}_"
        l2_colls = [c for c in all_collections if c.startswith(l2_prefix)]
        
        print(f"\nL2 Collections ({len(l2_colls)} found):")
        if l2_colls:
            for coll in sorted(l2_colls):
                count = db[coll].count_documents({})
                print(f"  ✓ {coll} ({count:,} documents)")
        else:
            print(f"  ❌ No L2 collections found")
    
    client.close()

if __name__ == "__main__":
    try:
        check_collections()
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
