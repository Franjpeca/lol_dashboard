import os
import sys
import time
import datetime
import argparse
from pathlib import Path
from dotenv import load_dotenv
from riotwatcher import RiotWatcher, LolWatcher, ApiError

# Configuración de rutas para importar utils si fuera necesario
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=BASE_DIR / ".env")

# Importar configuraciones del proyecto
sys.path.insert(0, str(BASE_DIR / "src"))
from utils.api_key_manager import get_api_key

def count_db_matches(puuid, pool_id="season", min_friends=1):
    try:
        import psycopg2
        dsn = os.getenv("POSTGRES_URI").replace("postgresql+psycopg2://", "postgresql://")
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        
        sql = """
            SELECT COUNT(*) 
            FROM player_performances 
            WHERE pool_id = %s 
              AND friends_count >= %s 
              AND puuid = %s
        """
        cur.execute(sql, (pool_id, min_friends, puuid))
        count = cur.fetchone()[0]
        conn.close()
        return count
    except Exception as e:
        return f"Error: {e}"

def count_flex_matches(riot_id, start_date_str="2026-01-08", end_date_str="2026-04-06"):
    try:
        api_key = get_api_key()
        watcher = RiotWatcher(api_key)
        lol_watcher = LolWatcher(api_key)
        region = os.getenv("REGIONAL_ROUTING", "europe")
        
        if "#" not in riot_id:
            print(f"Error: El Riot ID '{riot_id}' debe tener el formato Nombre#Tag")
            return

        name, tag = riot_id.split("#", 1)
        
        print(f"🔍 Buscando PUUID para {riot_id}...")
        account = watcher.account.by_riot_id(region, name.strip(), tag.strip())
        puuid = account['puuid']
        print(f"✅ PUUID encontrado: {puuid}")

        # Consulta API
        start_dt = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").replace(tzinfo=datetime.timezone.utc)
        end_dt = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=datetime.timezone.utc)
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        
        all_matches = []
        start_index = 0
        batch_size = 100
        
        print(f"🚀 Consultando partidas Flex (API: Queue 440)...")
        while True:
            matches = lol_watcher.match.matchlist_by_puuid(
                region, puuid, start=start_index, count=batch_size, 
                queue=440, start_time=start_ts, end_time=end_ts
            )
            if not matches: break
            all_matches.extend(matches)
            if len(matches) < batch_size: break
            start_index += batch_size
            time.sleep(0.2)

        # Consulta DB
        print(f"🗄️  Consultando base de datos local (Pool: season, min1)...")
        db_count = count_db_matches(puuid, "season", 1)

        print("\n" + "="*40)
        print(f"📊 RESULTADO PARA {riot_id}")
        print(f"📅 Rango API: {start_date_str} al {end_date_str}")
        print(f"🌐 API Riot (Flex): {len(all_matches)} partidas")
        print(f"🏠 DB Local (Season, min1): {db_count} partidas")
        print("="*40)

    except ApiError as err:
        print(f"❌ Error de API: {err}")
    except Exception as e:
        print(f"❌ Error inesperado: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cuenta partidas Flex en un rango de fechas.")
    parser.add_argument("riot_id", help="Riot ID en formato Nombre#Tag")
    parser.add_argument("--start", default="2026-01-08", help="Fecha inicio YYYY-MM-DD")
    parser.add_argument("--end", default="2026-04-06", help="Fecha fin YYYY-MM-DD")
    
    args = parser.parse_args()
    count_flex_matches(args.riot_id, args.start, args.end)
