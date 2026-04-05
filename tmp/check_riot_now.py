from pathlib import Path
import sys
from riotwatcher import RiotWatcher

# Rutas
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parents[1]
SRC_DIR = BASE_DIR / "src"
sys.path.insert(0, str(SRC_DIR))

from utils.api_key_manager import get_api_key
from utils.config import REGIONAL_ROUTING

def check_now(riot_id):
    print(f"--- Verificando {riot_id} contra Riot API ---")
    api_key = get_api_key(REGIONAL_ROUTING)
    riot = RiotWatcher(api_key)
    
    name, tag = riot_id.split("#")
    try:
        acc = riot.account.by_riot_id(REGIONAL_ROUTING, name.strip(), tag.strip())
        print(f"✅ CONEXIÓN EXITOSA")
        print(f"PUUID ACTUAL: {acc['puuid']}")
        print(f"GAME NAME: {acc['gameName']}#{acc['tagLine']}")
    except Exception as e:
        print(f"❌ ERROR: No se pudo encontrar a {riot_id} en Riot.")
        print(f"Detalle: {e}")

if __name__ == "__main__":
    # Probamos con el nombre exacto que tienes en el JSON
    check_now("Kayki マ#b0nk")
