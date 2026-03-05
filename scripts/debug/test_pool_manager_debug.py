from pathlib import Path
import sys
import os
import sys
from pathlib import Path

# Add src to path
current_dir = Path(__file__).resolve().parent
src_dir = current_dir / "src"
sys.path.insert(0, str(src_dir))

from utils.pool_manager import get_available_pools
from dotenv import load_dotenv

load_dotenv()

print("Testing get_available_pools...")
try:
    pools = get_available_pools(current_dir)
    print(f"Pools found: {pools}")
except Exception as e:
    print(f"Error calling get_available_pools: {e}")


# Setup paths
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parent
SRC_DIR = BASE_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utils.pool_manager import get_available_pools

pools = get_available_pools(BASE_DIR)
print(f"Available pools: {pools}")
