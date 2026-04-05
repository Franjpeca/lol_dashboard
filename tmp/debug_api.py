import sys
import datetime
from pathlib import Path

# Asegurar que src/ y la raíz estén en sys.path
FILE_SELF = Path(__file__).resolve()
ROOT_DIR = FILE_SELF.parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from riotwatcher import LolWatcher
from utils.api_key_manager import get_api_key

def check():
    key = get_api_key()
    if not key:
        print("❌ Error: No se encontró RIOT_API_KEY en el .env")
        return

    lol = LolWatcher(key)
    print("\n" + "=" * 40)
    print("🌐 [DEBUG] CONSULTA DIRECTA A RIOT API")
    print("=" * 40)
    
    try:
        # 1. Obtener PUUID actual
        print(f"🔍 Buscando cuenta: iryrell lover#WORDO...")
        acc = lol.account.by_riot_id('europe', 'iryrell lover', 'WORDO')
        puuid = acc['puuid']
        print(f"✅ Cuenta: {acc.get('gameName')}#{acc.get('tagLine')}")
        print(f"✅ PUUID: {puuid}")
        
        # 2. Obtener lista de partidas (últimas 10)
        match_ids = lol.match.matchlist_by_puuid('europe', puuid, count=10)
        print(f"\nÚltimas 10 partidas encontradas por Riot:")
        
        if not match_ids:
            print("❌ Riot dice que esta cuenta NO tiene partidas registradas.")
            return

        for mid in match_ids:
            data = lol.match.by_id('europe', mid)
            info = data['info']
            ts = info['gameStartTimestamp']
            dt = datetime.datetime.fromtimestamp(ts / 1000)
            q_id = info.get('queueId')
            print(f" - {mid} | Fecha: {dt.strftime('%d/%m/%Y %H:%M:%S')} | Cola: {q_id}")

    except Exception as e:
        print(f"❌ Error durante la consulta: {e}")

    print("=" * 40 + "\n")

if __name__ == "__main__":
    check()
