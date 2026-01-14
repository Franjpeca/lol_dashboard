
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

OLD_CODE = """def get_users_index_collection(pool_id: str):
    if pool_id == "season":
        return "L0_users_index_season"
    return "L0_users_index\""""

OLD_CODE_ALT = """def get_users_index_collection(pool_id: str):
    if pool_id == "season":
        return "L0_users_index_season"
    return "L0_users_index\""""

# Handle variations in whitespace or quotes if necessary
# But the grep output seemed consistent.
# Let's use a simpler replace strategy: read file, replace block.

NEW_CODE = """def get_users_index_collection(pool_id: str):
    return "L0_users_index\""""

def process_file(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # Try direct replacement
    if "L0_users_index_season" in content:
        # Regex or string replace?
        # The block is small, let's try to be smart.
        # Find the function def
        if "def get_users_index_collection(pool_id: str):" in content:
            # We want to replace the logic inside.
            # Since indentation might vary slightly, let's replace the specific if statement lines.
            
            new_content = content.replace('    if pool_id == "season":\n        return "L0_users_index_season"\n    return "L0_users_index"', '    return "L0_users_index"')
            
            if new_content == content:
                 # Try with different indentation or spacing?
                 # Or maybe manual construction
                 pass
            
            # Fallback: Replace just the string return if we can filter by context?
            # No, that's risky.
            
            # Let's try to find the EXACT lines from grep previous output context if possible, but I don't have it.
            # I'll rely on the standard formatting I saw in view_file.
            pass
            
            if new_content != content:
                print(f"Fixed: {file_path.name}")
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(new_content)
            else:
                print(f"Skipped (pattern not found exactly): {file_path.name}")

files = [
    "metrics_04_win_lose_streak.py",
    "metrics_12_botlane_synergy.py",
    "metrics_11_stats_record.py",
    "metrics_05_players_stats.py",
    "metrics_09_number_skills.py",
    "metrics_08_first_metrics.py",
    "metrics_13_player_champions_stats.py",
    "metrics_07_troll_index.py",
    "metrics_06_ego_index.py",
    "metrics_03_games_frecuency.py",
    "metrics_01_players_games_winrate.py",
    "metrics_10_stats_by_rol.py"
]

for fname in files:
    fpath = BASE_DIR / fname
    if fpath.exists():
        process_file(fpath)
    else:
        print(f"Not found: {fname}")
