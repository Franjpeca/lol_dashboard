import os
from pathlib import Path

def get_api_key(region="europe"):
    """
    Returns the API Key directly from the RIOT_API_KEY environment variable.
    No local storage or dynamic validation is used anymore.
    """
    key = os.getenv("RIOT_API_KEY")
    
    if not key:
        print("[API_KEY_MANAGER] ❌ ERROR: RIOT_API_KEY not found in environment (.env)")
        raise RuntimeError("Missing RIOT_API_KEY. Please set it in your .env file.")
        
    # Optional debug print (obfuscated)
    # print(f"[API_KEY_MANAGER] 🔑 Using key from .env ending in ...{key[-4:]}")
    
    return key.strip()