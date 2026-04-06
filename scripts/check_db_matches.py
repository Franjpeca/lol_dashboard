import os
import sys
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=BASE_DIR / ".env")

def check_db_matches(puuid, pool_id="season", min_friends=1):
    try:
        # DB connection
        dsn = os.getenv("POSTGRES_URI").replace("postgresql+psycopg2://", "postgresql://")
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        
        # Query
        sql = """
            SELECT COUNT(*) 
            FROM player_performances 
            WHERE pool_id = %s 
              AND friends_count >= %s 
              AND puuid = %s
        """
        cur.execute(sql, (pool_id, min_friends, puuid))
        count = cur.fetchone()[0]
        
        print("\n" + "="*40)
        print(f"🗄️  CONSULTA EN BASE DE DATOS LOCAL")
        print(f"👤 PUUID: {puuid}")
        print(f"📋 Pool: {pool_id}")
        print(f"👥 Min Amigos: {min_friends}")
        print(f"🎮 Total partidas encontradas: {count}")
        print("="*40)
        
        conn.close()
    except Exception as e:
        print(f"❌ Error consultando la base de datos: {e}")

if __name__ == "__main__":
    # PUUID de iryrell lover#WORDO
    target_puuid = "anwmg72zGLnXtcMvkKm12fzKvM574Lr2iO2nsz7uhSs4ivKr2xOBCp6Yzx_QLRloUeoTXS_sRxnUBw"
    check_db_matches(target_puuid)
