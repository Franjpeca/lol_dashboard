
import os
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

def get_available_pools(base_dir: Path) -> List[str]:
    """
    Scans MongoDB for L1 collections and extracts unique pool IDs.
    Returns a list of pool IDs (e.g., 'c9e438d4', 'd63e5437').
    """
    try:
        MONGO_URI = os.getenv("MONGO_URI")
        DB_NAME = os.getenv("MONGO_DB", "lol_data")
        
        if not MONGO_URI:
            print("[WARN] MONGO_URI not found in environment")
            return []
        
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        
        # Get all L1 collection names
        l1_collections = [c for c in db.list_collection_names() if c.startswith("L1_")]
        
        # Extract unique pool IDs from collection names
        # Format: L1_q440_min5_pool_c9e438d4
        pools = set()
        for coll_name in l1_collections:
            if "_pool_" in coll_name:
                pool_id = coll_name.split("_pool_")[1]
                pools.add(pool_id)
        
        client.close()
        return sorted(list(pools))
    
    except Exception as e:
        print(f"[ERROR] Failed to get pools from MongoDB: {e}")
        return []

def get_available_reports(base_dir: Path, pool_id: str, queue: int, min_friends: int) -> List[dict]:
    """
    Scans for generated metrics files to find available date ranges.
    Returns list of dicts: {"label": "All Time", "value": "all"}
    """
    reports = [{"label": "Datos completos", "value": "all"}]
    
    if not pool_id or pool_id == "auto":
        return reports
        
    try:
        # Check runtime folder for custom date ranges
        # data/runtime/pool_X/qY/minZ/metrics_01_players_games_winrate_START_to_END.json
        runtime_dir = base_dir / "data" / "runtime" / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}"
        
        if runtime_dir.exists():
            for f in runtime_dir.glob("metrics_01_players_games_winrate_*_to_*.json"):
                # Extract dates from filename
                # filename looks like: metrics_01_players_games_winrate_2024-01-01_to_2024-01-31.json
                name = f.stem
                if "_to_" in name:
                    parts = name.split("_")
                    # starting from end: ..._END
                    end_date = parts[-1]
                    # ..._START_to_END
                    start_date = parts[-3]
                    
                    label = f"{start_date} to {end_date}"
                    value = f"{start_date}|{end_date}"
                    reports.append({"label": label, "value": value})
                    
    except Exception as e:
        print(f"[WARN] Error scanning reports: {e}")
        
    return reports

