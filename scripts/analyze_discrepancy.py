import os
import sys
import psycopg2
import datetime
from dotenv import load_dotenv
from pathlib import Path
from riotwatcher import RiotWatcher, LolWatcher

# Paths
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=BASE_DIR / ".env")
sys.path.insert(0, str(BASE_DIR / "src"))

def analyze(riot_id, puuid):
    api_key = os.getenv("RIOT_API_KEY")
    watcher = LolWatcher(api_key)
    region = os.getenv("REGIONAL_ROUTING", "europe")
    
    # Rango de fechas solicitado
    start_dt = datetime.datetime(2026, 1, 8, tzinfo=datetime.timezone.utc)
    end_dt = datetime.datetime(2026, 4, 6, 23, 59, 59, tzinfo=datetime.timezone.utc)
    
    print("\n" + "="*60)
    print(f"📊 ANÁLISIS DE DISCREPANCIAS PARA: {riot_id}")
    print(f"📅 RANGO DE ANÁLISIS: {start_dt.strftime('%d/%m/%Y')} al {end_dt.strftime('%d/%m/%Y')}")
    print("="*60)

    # 1. Obtener partidas de la API (Referencia)
    print(f"\n🚀 Consultando API de Riot (Referencia)...")
    try:
        api_match_ids = watcher.match.matchlist_by_puuid(
            region, puuid, queue=440, 
            start_time=int(start_dt.timestamp()), 
            end_time=int(end_dt.timestamp()), 
            count=100
        )
        api_count = len(api_match_ids)
        api_set = set(api_match_ids)
        print(f"✅ Partidas Flex encontradas en la API: {api_count}")
    except Exception as e:
        print(f"❌ Error consultando la API: {e}")
        return

    # 2. Capa L0: MongoDB (Partidas Crudas)
    print(f"\n📦 Consultando Capa L0 (MongoDB - {api_count} partidas en rango)...")
    from pymongo import MongoClient
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db_name = os.getenv("MONGO_DB", "lol_data")
    mongo_coll_name = os.getenv("MONGO_COLLECTION_RAW_MATCHES", "L0_all_raw_matches")
    
    mongo_exists = []
    mongo_missing = []
    try:
        m_client = MongoClient(mongo_uri)
        m_db = m_client[mongo_db_name]
        m_coll = m_db[mongo_coll_name]
        
        for mid in api_match_ids:
            if m_coll.find_one({"_id": mid}):
                mongo_exists.append(mid)
            else:
                mongo_missing.append(mid)
        
        print(f"✅ Estan en MongoDB (L0): {len(mongo_exists)}")
        print(f"❌ Faltan en MongoDB (L0): {len(mongo_missing)}")
    except Exception as e:
        print(f"⚠️  Error consultando MongoDB: {e}")
        mongo_exists = []

    # 3. Capa L1/L2: PostgreSQL (Partidas Procesadas en Pool 'season')
    print(f"\n🗄️  Consultando Capa L1/L2 (PostgreSQL - Pool 'season')...")
    dsn = os.getenv("POSTGRES_URI").replace("postgresql+psycopg2://", "postgresql://")
    try:
        conn = psycopg2.connect(dsn)
        cur = conn.cursor()
        
        # Consultamos las partidas asociadas a ESTE puuid concreto en ESTA pool
        cur.execute("SELECT match_id FROM player_performances WHERE pool_id = 'season' AND puuid = %s", (puuid,))
        db_results = cur.fetchall()
        db_set = {r[0] for r in db_results}
        db_count = len(db_set)
        
        missing_in_pg = set(api_match_ids) - db_set
        dif_count = len(missing_in_pg)

        print(f"✅ Partidas en PostgreSQL (Pool 'season'): {db_count}")
        print(f"❌ Partidas FALTANTES en PostgreSQL: {dif_count}")
        print(f"📊 Diferencia Total: {api_count} (API) - {db_count} (DB) = {dif_count} faltantes")
        
        if missing_in_pg:
            print("\n🔍 LISTADO DE PARTIDAS FALTANTES (Muestra de 5):")
            for mid in sorted(list(missing_in_pg))[:5]:
                print(f"   - {mid}")
        
        # Caso específico solicitado por el usuario
        target_mid = "EUW1_7750244844"
        print(f"\n🔎 VERIFICACIÓN DE PARTIDA ESPECÍFICA: {target_mid}")
        
        # Check Mongo L1 (Filtro)
        l1_name = "L1_q440_min1_pool_season"
        l1_coll = m_db[l1_name]
        l1_doc = l1_coll.find_one({"_id": target_mid})
        if l1_doc:
            print(f"   - ¿Está en Mongo L1 (season)? SÍ")
            f_list = l1_doc.get('friends_present', [])
            print(f"   - Amigos detectados (PUUIDs): {len(f_list)}")
            print(f"   - ¿Está el PUUID de {riot_id} ({puuid}) en la lista? {'SÍ ✅' if puuid in f_list else 'NO ❌'}")
        else:
            print(f"   - ¿Está en Mongo L1 (season)? NO ❌")

        conn.close()
    except Exception as e:
        print(f"⚠️  Error consultando PostgreSQL: {e}")

    print("\n" + "="*60)
    print("🏁 FIN DEL ANÁLISIS")
    print("="*60 + "\n")

if __name__ == "__main__":
    player_riot_id = "iryrell lover#WORDO"
    player_puuid = "anwmg72zGLnXtcMvkKm12fzKvM574Lr2iO2nsz7uhSs4ivKr2xOBCp6Yzx_QLRloUeoTXS_sRxnUBw"
    analyze(player_riot_id, player_puuid)
