
import sys
import os
from pathlib import Path

# Configurar rutas
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from utils.config import RIOT_API_KEY, POSTGRES_URI, MONGO_DB, COLLECTION_RAW_MATCHES
from utils.db import get_mongo_client
from riotwatcher import LolWatcher, ApiError
import psycopg2
import pandas as pd

from utils.config import RIOT_API_KEY, POSTGRES_URI, MONGO_DB, COLLECTION_RAW_MATCHES
from utils.db import get_mongo_client
from riotwatcher import RiotWatcher, LolWatcher, ApiError
import psycopg2
import pandas as pd

def main():
    # 1. Configuración
    rw_account = RiotWatcher(RIOT_API_KEY)
    rw_matches = LolWatcher(RIOT_API_KEY)
    region = "europe"
    riot_id = "Riyo Reaper#wajoo"
    pool_id = "imperio_itzantino"
    min_friends = 3
    queue_id = 440
    
    print(f"🔍 Iniciando auditoría para: {riot_id}")
    print(f"📊 Comparando con pool: {pool_id} (min_friends={min_friends}, queue={queue_id})")

    # 2. Obtener PUUID
    name, tag = riot_id.split("#")
    try:
        acc = rw_account.account.by_riot_id(region, name, tag)
        puuid = acc["puuid"]
        print(f"✅ PUUID encontrado: {puuid}")
    except ApiError as e:
        print(f"❌ Error al buscar cuenta en Riot: {e}")
        return

    # 3. Obtener últimas 105 partidas de Riot (Filtrando por FLEX = 440)
    # Riot solo permite 100 por llamada, así que hacemos dos.
    print(f"📡 Pidiendo últimas 105 partidas FLEX (Queue 440) a Riot...")
    try:
        m1 = rw_matches.match.matchlist_by_puuid(region, puuid, start=0, count=100, queue=440)
        m2 = rw_matches.match.matchlist_by_puuid(region, puuid, start=100, count=5, queue=440)
        match_ids = m1 + m2
        print(f"✅ Se han encontrado {len(match_ids)} partidas FLEX en Riot.")
    except ApiError as e:
        print(f"❌ Error al pedir matchlist: {e}")
        return

    # 4. Obtener campeones de esas partidas (Usando Mongo para ir rápido)
    print("🧪 Verificando campeones en MongoDB/Riot...")
    riot_counts = {}
    
    with get_mongo_client() as client:
        db = client[MONGO_DB]
        coll_raw = db[COLLECTION_RAW_MATCHES]

        for mid in match_ids:
            # Intentar sacar de Mongo primero
            match_data = coll_raw.find_one({"metadata.matchId": mid})
            if not match_data:
                # Si no está en Mongo, esto es un problema de ingesta
                champion = "RIOTEADO (No en Mongo)"
            else:
                # Buscar al jugador en la partida
                participants = match_data["info"]["participants"]
                champion = next((p["championName"] for p in participants if p["puuid"] == puuid), "???")
            
            riot_counts[champion] = riot_counts.get(champion, 0) + 1

    # 5. Consultar PostgreSQL
    print("🐘 Consultando base de datos PostgreSQL...")
    pg_dsn = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")
    db_counts = {}
    table_name = f"L1_q{queue_id}_min{min_friends}_pool_{pool_id}"
    
    try:
        conn = psycopg2.connect(pg_dsn)
        cur = conn.cursor()
        
        query = f"""
            SELECT champion_name, COUNT(*) as total 
            FROM {table_name} 
            WHERE puuid = %s 
            GROUP BY champion_name
        """
        cur.execute(query, (puuid,))
        rows = cur.fetchall()
        for champ, total in rows:
            db_counts[champ] = total
        conn.close()
    except Exception as e:
        print(f"⚠️ Error al consultar Postgres (¿Existe la tabla {table_name}?): {e}")

    # 6. Mostrar resultados por separado
    print("\n" + "🔵" * 20)
    print(f"   RESULTADOS RIOT API (Últimas 105 partidas)")
    print("🔵" * 20)
    riot_sorted = sorted(riot_counts.items(), key=lambda x: x[1], reverse=True)
    for champ, count in riot_sorted:
        print(f" - {champ:<15}: {count} partidas")

    print("\n" + "🐘" * 20)
    print(f"   RESULTADOS BASE DE DATOS (Pool: {pool_id})")
    print("🐘" * 20)
    if not db_counts:
        print(" ❌ No se han encontrado registros en esta tabla.")
    else:
        db_sorted = sorted(db_counts.items(), key=lambda x: x[1], reverse=True)
        for champ, count in db_sorted:
            print(f" - {champ:<15}: {count} partidas")

    # 7. Comparativa final
    print("\n" + "⚖️" * 20)
    print(f"      TABLA COMPARATIVA FINAL")
    print("⚖️" * 20)
    print(f"{'CAMPEÓN':<20} | {'RIOT':<10} | {'DB':<10} | {'DIFERENCIA'}")
    print("-" * 55)
    
    all_champs = sorted(set(list(riot_counts.keys()) + list(db_counts.keys())))
    for c in all_champs:
        r = riot_counts.get(c, 0)
        d = db_counts.get(c, 0)
        diff = d - r
        icon = "✅" if diff == 0 else "❌" if diff < 0 else "➕"
        print(f"{c:<20} | {r:<10} | {d:<10} | {diff:+d} {icon}")
    print("="*55)

if __name__ == "__main__":
    main()
