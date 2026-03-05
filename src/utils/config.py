
import os
from pathlib import Path
from dotenv import load_dotenv

# Internal Paths
FILE_SELF = Path(__file__).resolve()
SRC_DIR = FILE_SELF.parents[1]
BASE_DIR = SRC_DIR.parent
RESULTS_ROOT = BASE_DIR / "data" / "results"
RUNTIME_ROOT = BASE_DIR / "data" / "runtime"

# Load .env only once when this module is imported
env_path = BASE_DIR / ".env"
load_dotenv(dotenv_path=env_path)

# ================================
# MONGO DATABASE CONFIG (L0 — datos crudos)
# ================================
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "lol_data")

# Default Collections
COLLECTION_RAW_MATCHES = os.getenv("MONGO_COLLECTION_RAW_MATCHES", "L0_all_raw_matches")
COLLECTION_ACCOUNTS = "riot_accounts"
COLLECTION_USERS_INDEX = "L0_users_index"

# ================================
# POSTGRESQL CONFIG (L1/L2/métricas — datos procesados)
# ================================
POSTGRES_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql+psycopg2://lol_user:lol_pass@localhost:5432/lol_analytics"
)

# ================================
# RIOT API CONFIG
# ================================
REGIONAL_ROUTING = os.getenv("REGIONAL_ROUTING", "europe")
QUEUE_FLEX = int(os.getenv("QUEUE_FLEX", "440"))
MIN_FRIENDS_IN_MATCH = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))

COUNT_PER_PLAYER = int(os.getenv("COUNT_PER_PLAYER", "800"))
SLEEP_BETWEEN_CALLS = float(os.getenv("SLEEP_BETWEEN_CALLS", "0.2"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "6"))

# ================================
# PATHS
# ================================
LOL_CACHE_DIR = os.getenv("LOL_CACHE_DIR")
LOL_USERS_DIR = os.getenv("LOL_USERS_DIR")
LOL_PLAYERS_FILE = os.getenv("LOL_PLAYERS_FILE")

# Optional: Ensure base paths exist or format them as Path objects if needed by scripts
if LOL_CACHE_DIR:
    PATH_LOL_CACHE = Path(LOL_CACHE_DIR)
else:
    PATH_LOL_CACHE = None

if LOL_USERS_DIR:
    PATH_LOL_USERS = Path(LOL_USERS_DIR)
else:
    PATH_LOL_USERS = None

if LOL_PLAYERS_FILE:
    PATH_LOL_PLAYERS = Path(LOL_PLAYERS_FILE)
else:
    PATH_LOL_PLAYERS = None

