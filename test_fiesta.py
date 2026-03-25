import sys
from pathlib import Path
from unittest.mock import MagicMock

# Mock streamlit before importing db.py
sys.modules["streamlit"] = MagicMock()

# Add ROOT and src to path
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

try:
    from dashboard.db import get_fiesta_stats
    import pandas as pd

    # Test with known pool
    pool_id = "season"
    print(f"Testing fiesta stats for pool: {pool_id}")
    df = get_fiesta_stats(pool_id, 440, 5)

    if df.empty:
        print("Warning: DataFrame is empty. This might be due to lack of data or minimum game filter (5).")
    else:
        print("Success! Data retrieved:")
        print(df.head())
        print("\nColumns:", df.columns.tolist())
        
except Exception as e:
    print(f"Error during verification: {e}")
    import traceback
    traceback.print_exc()
