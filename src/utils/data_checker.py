import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[2] # lol_data/
DATA_DIR = BASE_DIR / "data" / "results"

def check_data_availability(pool_id: str, queue: int, min_friends: int) -> bool:
    """
    Checks if the essential JSON files exist for the given configuration.
    Returns True if data seems available, False otherwise.
    """
    if not pool_id:
        return False
        
    path = DATA_DIR / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}"
    
    # Check for at least one key file
    required_files = [
        "metrics_01_players_games_winrate.json",
        # We could add more, but if this one exists, the pipeline likely ran
    ]
    
    if not path.exists():
        return False
        
    for filename in required_files:
        if not (path / filename).exists():
            return False
            
    return True
