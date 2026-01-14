import sys
import os
from pathlib import Path

# Add src to path
BASE = Path(__file__).resolve().parents[1]
SRC = BASE / "src"
sys.path.insert(0, str(SRC))

from utils.api_key_manager import save_new_temp_key, get_api_key
from multiprocessing import Queue

def test_api_priority():
    print("\n[TEST] 1. Testing API Key Priority")
    
    # 1. Check env key
    env_key = os.getenv("RIOT_API_KEY")
    print(f"   Current .env key: ...{env_key[-6:] if env_key else 'None'}")
    
    # 2. Save new key
    new_key = "RGAPI-0e3bdce8-cab5-4939-b3ae-326ebe4af16c"
    print(f"   Saving NEW key:   ...{new_key[-6:]}")
    
    # NOTE: save_new_temp_key tries to validate with Riot. 
    # If network is down or key is bad, it might fail. 
    # We will see the output.
    result = save_new_temp_key(new_key)
    print(f"   Save Result: {result}")
    
    if not result["success"]:
        print("   [WARN] Could not save key (maybe invalid?). Skipping priority check if save failed.")
        # Even if validation fails, the user might want us to proceed? 
        # But save_new_temp_key ensures only valid keys are saved.
    
    # 3. Get active key
    try:
        active_key = get_api_key()
        print(f"   Active Key returned: ...{active_key[-6:]}")
        
        if active_key == new_key:
            print("   ✅ PASS: Active key matches the NEW saved key (Priority Correct)")
        elif active_key == env_key:
            print("   ❌ FAIL: Active key matches .env key (Priority Incorrect)")
        else:
            print("   ❓ INFO: Active key is neither .env nor new key (maybe another saved key?)")
            
    except Exception as e:
        print(f"   ❌ ERROR getting api key: {e}")

def test_pipeline_import():
    print("\n[TEST] 2. Testing Pipeline Imports")
    try:
        from run_pipeline import run_l0_only, run_l1_to_l3, main_full
        print("   ✅ PASS: run_pipeline module imported successfully")
        print(f"   Functions available: run_l0_only, run_l1_to_l3, main_full")
    except ImportError as e:
        print(f"   ❌ FAIL: Could not import run_pipeline: {e}")

if __name__ == "__main__":
    print("=== STARTING INTEGRATION TEST ===")
    test_api_priority()
    test_pipeline_import()
    print("\n=== TEST FINISHED ===")
