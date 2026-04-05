import sys
import argparse
import datetime
from pathlib import Path

# Asegurar que src/ esté en sys.path
FILE_SELF = Path(__file__).resolve()
SRC_DIR = FILE_SELF.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utils.db import get_mongo_client
from utils.config import MONGO_DB, COLLECTION_RAW_MATCHES, COLLECTION_ACCOUNTS, POSTGRES_URI
import psycopg2
import psycopg2.extras

def resolve_puuid(db, riot_id):
    """Busca el PUUID en las colecciones de índices de usuario (Normal y Season)."""
    # 1. Buscar en colecciones de índices (Normal y Season)
    for coll_name in ["L0_users_index", "L0_users_index_season"]:
        coll = db[coll_name]
        # Buscamos en el array 'accounts' donde coincida el riotId
        doc = coll.find_one({"accounts.riotId": {"$regex": f"^{riot_id}$", "$options": "i"}})
        if doc:
            # Encontrar el objeto exacto en el array
            for acc in doc.get("accounts", []):
                if acc.get("riotId", "").lower() == riot_id.lower():
                    return acc["puuid"], acc["riotId"]
    
    # 2. Fallback a la caché de cuentas (riot_accounts)
    coll_acc = db[COLLECTION_ACCOUNTS]
    doc_acc = coll_acc.find_one({"riotId": {"$regex": f"^{riot_id}$", "$options": "i"}})
    if doc_acc:
        return doc_acc["puuid"], doc_acc["riotId"]
        
    return None, None

def check_match_postgres(match_id):
    """Verifica si la partida existe en PostgreSQL."""
    try:
        pg_dsn = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")
        with psycopg2.connect(pg_dsn) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT match_id, duration_s, friends_present FROM matches WHERE match_id = %s", (match_id,))
                return cur.fetchone()
    except Exception as e:
        print(f"[PG ERROR] {e}")
        return None

def main():
    parser = argparse.ArgumentParser(description="Busca partidas de un jugador en una fecha específica.")
    parser.add_argument("--date", required=True, help="Fecha en formato DD/MM/YYYY (ej: 04/04/2026)")
    parser.add_argument("--riot-id", required=True, help="Nombre#Tag del jugador (ej: Fran#EUW)")
    parser.add_argument("--live", action="store_true", help="Consultar TAMBIÉN la API de Riot en vivo para ver qué dice.")
    args = parser.parse_args()

    # 1. Parsear fecha
    try:
        target_date = datetime.datetime.strptime(args.date, "%d/%m/%Y")
        start_dt_obj = target_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=datetime.timezone.utc)
        start_ts = int(start_dt_obj.timestamp() * 1000)
        end_ts = start_ts + (24 * 60 * 60 * 1000)
    except Exception as e:
        print(f"❌ Error en formato de fecha: {e}")
        return

    print(f"🔍 Buscando actividad de '{args.riot_id}' el día {args.date}...")

    with get_mongo_client() as client:
        db = client[MONGO_DB]
        
        # 2. Resolver PUUID
        puuid, real_id = resolve_puuid(db, args.riot_id)
        if not puuid:
            print(f"❌ No se encontró el jugador '{args.riot_id}' en la base de datos local (L0_accounts).")
            print("   Prueba a ejecutar: python src/extract/ingest_users.py primero.")
            return
        
        print(f"✅ Jugador identificado: {real_id} ({puuid})")

        # 3. Comprobación Opcional en Vivo (Riot API)
        if args.live:
            from riotwatcher import LolWatcher, ApiError
            from utils.api_key_manager import get_api_key
            from utils.config import REGIONAL_ROUTING, QUEUE_FLEX
            
            print(f"\n🌐 [LIVE] Consultando Riot API (sin filtros de cola)...")
            try:
                lol = LolWatcher(get_api_key(REGIONAL_ROUTING))
                # Buscamos las últimas 20 partidas sin filtro de cola
                live_ids = lol.match.matchlist_by_puuid(REGIONAL_ROUTING, puuid, count=20)
                
                print(f"   Últimas 20 partidas en Riot API:")
                found_live = 0
                for mid in live_ids:
                    m_data = lol.match.by_id(REGIONAL_ROUTING, mid)
                    info = m_data["data"]["info"] if "data" in m_data else m_data["info"]
                    ts = info["gameStartTimestamp"]
                    q_id = info["queueId"]
                    
                    if ts >= start_ts and ts < end_ts:
                        found_live += 1
                        is_flex = "SÍ" if q_id == QUEUE_FLEX else f"NO (Cola {q_id})"
                        status_local = "En Mongo ✅" if db[COLLECTION_RAW_MATCHES].find_one({"_id": mid}) else "MISSING ❌"
                        h = datetime.datetime.fromtimestamp(ts / 1000).strftime("%H:%M:%S")
                        print(f"    - {mid} | Hora: {h} | Flex? {is_flex} | Local: {status_local}")
                
                if found_live == 0:
                    print("   ❌ Riot API no reporta NINGUNA partida para este jugador hoy.")
            except ApiError as e:
                print(f"   ❌ Error al consultar API de Riot: {e}")
            except Exception as e:
                print(f"   ❌ Error inesperado en LIVE: {e}")

        # 3. Buscar en MongoDB
        query = {
            "metadata.participants": puuid,
            "data.info.gameStartTimestamp": {"$gte": start_ts, "$lt": end_ts}
        }
        
        matches = list(db[COLLECTION_RAW_MATCHES].find(query).sort("data.info.gameStartTimestamp", 1))
        
        if not matches:
            print(f"\n❄️  No se han encontrado partidas en MongoDB para ese día.")
            print("   Si jugaste hoy, intenta ejecutar: python src/run_all.py")
            return

        print(f"\n📦 Se han encontrado {len(matches)} partidas en MongoDB (Capa L0):")
        print("-" * 80)
        
        for m in matches:
            m_id = m["metadata"]["matchId"]
            info = m["data"]["info"]
            start_dt = datetime.datetime.fromtimestamp(info["gameStartTimestamp"] / 1000).strftime("%H:%M:%S")
            queue_id = info.get("queueId")
            queue_name = "Flex" if queue_id == 440 else (f"Otro ({queue_id})" if queue_id else "Desconocido")
            
            # Contar amigos presentes
            # Necesitamos saber quiénes son "amigos" según el índice
            # Pero para simplificar, buscaremos cuántos de los participantes están en COLLECTION_ACCOUNTS
            participants = m["metadata"]["participants"]
            friends_count = db[COLLECTION_ACCOUNTS].count_documents({"puuid": {"$in": participants}})
            
            status_pg = "✅ LISTA" if check_match_postgres(m_id) else "❌ NO PROCESADA"
            
            print(f"Match: {m_id} | Hora: {start_dt} | Cola: {queue_name} | Amigos: {friends_count}")
            print(f"      Estado Dashboard: {status_pg}")
            
            if friends_count < 5:
                print(f"      ⚠️  Nota: Al haber solo {friends_count} amigos, es probable que se filtre (mínimo suele ser 5).")
            if queue_id != 440:
                print(f"      ⚠️  Nota: No es una partida Flex (440). El dashboard suele mostrar solo Flex.")
        
        print("-" * 80)
        print("\nInterpretación:")
        print(" - Si sale 'NO PROCESADA': Ejecuta python src/run_all.py para moverla de Mongo a Postgres.")
        print(" - Si sale 'LISTA' pero no la ves: Verifica los filtros (Min Friends) en el Dashboard.")

if __name__ == "__main__":
    main()
