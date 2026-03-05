from pathlib import Path
import sys
import os

# Setup paths
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parent
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utils.pool_manager import get_available_pools

pools = get_available_pools(BASE_DIR)
print(f"Available pools: {pools}")
