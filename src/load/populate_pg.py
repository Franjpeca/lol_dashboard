"""
load/populate_pg.py
ETL: Lee colecciones L1 de MongoDB → escribe matches + player_performances en PostgreSQL.

Este script sustituye las colecciones Mongo L1/L2 por tablas SQL.
No toca L0_all_raw_matches ni L0_users_index (esos permanecen en Mongo).

Uso:
    python load/populate_pg.py                      # auto-detecta pool de L0_users_index
    python load/populate_pg.py --pool ca879f16      # pool específica
    python load/populate_pg.py --pool season        # pool season
"""

import sys
import argparse
import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding='utf-8')

FILE_SELF = Path(__file__).resolve()
SRC_DIR = FILE_SELF.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import psycopg2
import psycopg2.extras

from utils.config import (
    MONGO_DB, COLLECTION_USERS_INDEX,
    QUEUE_FLEX, MIN_FRIENDS_IN_MATCH, POSTGRES_URI
)
from utils.db import get_mongo_client
from utils.pool_manager import build_pool_version

# psycopg2 necesita DSN sin el prefijo SQLAlchemy
_PG_DSN = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")


def now_utc():
    return datetime.datetime.now(datetime.timezone.utc)


def ts_ms_to_dt(ts_ms: int | None):
    if ts_ms is None:
        return None
    try:
        return datetime.datetime.fromtimestamp(ts_ms / 1000, tz=datetime.timezone.utc)
    except Exception:
        return None


# =============================================================
# POOL helpers
# =============================================================

def resolve_pool(mongo_db, pool_arg: str | None, queue_id: int, min_friends: int) -> tuple[str, str]:
    """Devuelve (pool_id_str, l1_collection_name)."""
    if pool_arg:
        pool_id = pool_arg
        l1_name = f"L1_q{queue_id}_min{min_friends}_pool_{pool_id}"
        return pool_id, l1_name

    # Auto-calcular desde L0_users_index
    personas = set()
    for doc in mongo_db[COLLECTION_USERS_INDEX].find({}, {"persona": 1}):
        p = doc.get("persona")
        if p:
            personas.add(p)

    if not personas:
        raise RuntimeError("No se encontraron personas en L0_users_index")

    pool_version = build_pool_version(sorted(list(personas)))  # 'pool_ca879f16'
    pool_id = pool_version.replace("pool_", "")
    l1_name = f"L1_q{queue_id}_min{min_friends}_{pool_version}"
    return pool_id, l1_name


def get_personas_list(mongo_db, collection: str) -> list[str]:
    return [doc["persona"] for doc in mongo_db[collection].find({}, {"persona": 1})]


# =============================================================
# PG helpers
# =============================================================

def ensure_pool(pg_conn, pool_id: str, personas: list[str], queue_id: int, min_friends: int):
    with pg_conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pools (pool_id, min_friends, personas, queue_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (pool_id, min_friends) DO UPDATE
            SET personas = EXCLUDED.personas
        """, (pool_id, min_friends, personas, queue_id))


MATCH_UPSERT = """
    INSERT INTO matches (
        match_id, pool_id, queue_id, min_friends,
        duration_s, game_start_ts, game_start_at, game_end_at,
        friends_present, personas_present, winning_team, filtered_at
    ) VALUES (
        %(match_id)s, %(pool_id)s, %(queue_id)s, %(min_friends)s,
        %(duration_s)s, %(game_start_ts)s, %(game_start_at)s, %(game_end_at)s,
        %(friends_present)s, %(personas_present)s, %(winning_team)s, %(filtered_at)s
    )
    ON CONFLICT (match_id, pool_id) DO UPDATE SET
        friends_present  = EXCLUDED.friends_present,
        personas_present = EXCLUDED.personas_present,
        winning_team     = EXCLUDED.winning_team,
        filtered_at      = EXCLUDED.filtered_at
"""

PP_UPSERT = """
    INSERT INTO player_performances (
        match_id, puuid, persona, is_friend,
        champion_name, team_id, win, lane, role,
        kills, deaths, assists, gold_earned,
        damage_dealt, damage_taken, vision_score,
        damage_mitigated, cs_total, riot_id_name, game_ended_surrender,
        pool_id, queue_id, game_start_at, duration_s,
        friends_count,
        first_blood_kill, first_blood_assist, longest_time_spent_living,
        takedowns_first_x_minutes, gold_per_minute, damage_per_minute,
        vision_score_per_minute, lane_minions_first_10_minutes,
        spell1_casts, spell2_casts, spell3_casts, spell4_casts
    ) VALUES (
        %(match_id)s, %(puuid)s, %(persona)s, %(is_friend)s,
        %(champion_name)s, %(team_id)s, %(win)s, %(lane)s, %(role)s,
        %(kills)s, %(deaths)s, %(assists)s, %(gold_earned)s,
        %(damage_dealt)s, %(damage_taken)s, %(vision_score)s,
        %(damage_mitigated)s, %(cs_total)s, %(riot_id_name)s, %(game_ended_surrender)s,
        %(pool_id)s, %(queue_id)s, %(game_start_at)s, %(duration_s)s,
        %(friends_count)s,
        %(first_blood_kill)s, %(first_blood_assist)s, %(longest_time_spent_living)s,
        %(takedowns_first_x_minutes)s, %(gold_per_minute)s, %(damage_per_minute)s,
        %(vision_score_per_minute)s, %(lane_minions_first_10_minutes)s,
        %(spell1_casts)s, %(spell2_casts)s, %(spell3_casts)s, %(spell4_casts)s
    )
    ON CONFLICT (match_id, pool_id, puuid) DO UPDATE SET
        persona = EXCLUDED.persona,
        is_friend = EXCLUDED.is_friend,
        win = EXCLUDED.win,
        friends_count = EXCLUDED.friends_count,
        riot_id_name = EXCLUDED.riot_id_name,
        role = EXCLUDED.role,
        lane = EXCLUDED.lane
"""


# =============================================================
# ETL MAIN
# =============================================================

def populate(pool_id: str, l1_name: str, queue_id: int, min_friends: int,
             mongo_db, pg_conn, users_collection: str = "L0_users_index"):

    print(f"[ETL] pool={pool_id} | l1={l1_name}")

    if l1_name not in mongo_db.list_collection_names():
        print(f"[ETL] ⚠️  L1 collection {l1_name} no existe en MongoDB, saltando.")
        return

    # Mapa puuid → persona
    puuid_to_persona: dict[str, str] = {}
    for doc in mongo_db[users_collection].find({}, {"persona": 1, "puuids": 1}):
        for p in doc.get("puuids", []):
            puuid_to_persona[p] = doc["persona"]

    personas_list = list(set(puuid_to_persona.values()))
    ensure_pool(pg_conn, pool_id, personas_list, queue_id, min_friends)
    pg_conn.commit()

    coll = mongo_db[l1_name]
    total_matches = 0
    total_pp = 0
    match_rows = []
    pp_rows = []

    cursor = coll.find({}, {
        "_id": 1, "data": 1, "friends_present": 1, "personas_present": 1,
        "queue": 1, "min_friends": 1, "pool_version": 1,
    })

    for doc in cursor:
        match_id = doc["_id"]
        data = doc.get("data", {})
        info = data.get("info", {})
        participants = info.get("participants", [])
        teams = info.get("teams", [])

        game_start_ts = info.get("gameStartTimestamp")
        game_end_ts = info.get("gameEndTimestamp")
        duration_s = info.get("gameDuration")
        friends_present = doc.get("friends_present", [])

        # winning team
        winning_team = None
        for t in teams:
            if t.get("win"):
                winning_team = t.get("teamId")
                break

        match_rows.append({
            "match_id": match_id,
            "pool_id": pool_id,
            "queue_id": doc.get("queue", queue_id),
            "min_friends": doc.get("min_friends", min_friends),
            "duration_s": duration_s,
            "game_start_ts": game_start_ts,
            "game_start_at": ts_ms_to_dt(game_start_ts),
            "game_end_at": ts_ms_to_dt(game_end_ts),
            "friends_present": friends_present,
            "personas_present": doc.get("personas_present", []),
            "winning_team": winning_team,
            "filtered_at": now_utc(),
        })
        total_matches += 1

        game_start_dt = ts_ms_to_dt(game_start_ts)
        # Track personas already inserted for this match (multi-cuenta dedup)
        personas_in_match: set = set()

        for p in participants:
            puuid = p.get("puuid")
            is_friend = puuid in friends_present
            persona = puuid_to_persona.get(puuid) if is_friend else None

            # Riot ID Name (GameName#TagLine)
            # Name resolution
            if p.get("riotIdGameName") and p.get("riotIdTagLine"):
                riot_id_name = f"{p['riotIdGameName']}#{p['riotIdTagLine']}"
            elif p.get("riotIdGameName"):
                riot_id_name = p["riotIdGameName"]
            else:
                riot_id_name = p.get("summonerName") or "Unknown"

            # Skip if we already have a row for this persona in this match
            if persona is not None and persona in personas_in_match:
                continue
            if persona is not None:
                personas_in_match.add(persona)

            cs = (p.get("totalMinionsKilled") or 0) + (p.get("neutralMinionsKilled") or 0)
            surrender = p.get("gameEndedInSurrender", False) or p.get("gameEndedInEarlySurrender", False)
            
            challenges = p.get("challenges", {})

            pp_rows.append({
                "match_id": match_id,
                "puuid": puuid,
                "persona": persona,
                "is_friend": is_friend,
                "champion_name": p.get("championName"),
                "team_id": p.get("teamId"),
                "win": p.get("win"),
                "lane": p.get("lane"),
                # Uso de teamPosition prioritario sobre role (MatchV5)
                "role": p.get("teamPosition") or p.get("role"),
                "kills": p.get("kills"),
                "deaths": p.get("deaths"),
                "assists": p.get("assists"),
                "gold_earned": p.get("goldEarned"),
                "damage_dealt": p.get("totalDamageDealtToChampions"),
                "damage_taken": p.get("totalDamageTaken"),
                "vision_score": p.get("visionScore"),
                "damage_mitigated": p.get("damageSelfMitigated"),
                "cs_total": cs,
                "riot_id_name": riot_id_name,
                "game_ended_surrender": surrender,
                "pool_id": pool_id,
                "queue_id": queue_id,
                "game_start_at": game_start_dt,
                "duration_s": duration_s,
                "friends_count": len(friends_present),
                "first_blood_kill": p.get("firstBloodKill", False),
                "first_blood_assist": p.get("firstBloodAssist", False),
                "longest_time_spent_living": p.get("longestTimeSpentLiving", 0),
                "takedowns_first_x_minutes": challenges.get("takedownsFirstXMinutes", 0),
                "gold_per_minute": challenges.get("goldPerMinute", 0),
                "damage_per_minute": challenges.get("damagePerMinute", 0),
                "vision_score_per_minute": challenges.get("visionScorePerMinute", 0),
                "lane_minions_first_10_minutes": challenges.get("laneMinionsFirst10Minutes", 0),
                "spell1_casts": p.get("spell1Casts", 0),
                "spell2_casts": p.get("spell2Casts", 0),
                "spell3_casts": p.get("spell3Casts", 0),
                "spell4_casts": p.get("spell4Casts", 0),
            })
            total_pp += 1

        # Flush in batches
        if len(match_rows) >= 500:
            with pg_conn.cursor() as cur:
                psycopg2.extras.execute_batch(cur, MATCH_UPSERT, match_rows, page_size=500)
                psycopg2.extras.execute_batch(cur, PP_UPSERT, pp_rows, page_size=500)
            pg_conn.commit()
            match_rows.clear()
            pp_rows.clear()

    # Final flush
    if match_rows:
        with pg_conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, MATCH_UPSERT, match_rows, page_size=500)
            psycopg2.extras.execute_batch(cur, PP_UPSERT, pp_rows, page_size=500)
        pg_conn.commit()

    print(f"[ETL] ✅ {total_matches} partidas | {total_pp} player_performances cargados")


def main():
    parser = argparse.ArgumentParser(description="ETL MongoDB L1 → PostgreSQL")
    parser.add_argument("--pool", type=str, default=None)
    parser.add_argument("--queue", type=int, default=QUEUE_FLEX)
    parser.add_argument("--min", type=int, default=MIN_FRIENDS_IN_MATCH)
    parser.add_argument("--users-collection", type=str, default="L0_users_index")
    args = parser.parse_args()

    print(f"[ETL] Arrancando populate_pg.py | queue={args.queue} min={args.min} pool={args.pool}")

    pg_conn = psycopg2.connect(_PG_DSN)
    pg_conn.autocommit = True

    # AUTO-FIX: Asegurar que la columna riot_id_name existe
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='player_performances' AND column_name='riot_id_name'
        """)
        if not cur.fetchone():
            print("[ETL] 🔧 Añadiendo columna 'riot_id_name' a player_performances...")
            cur.execute("ALTER TABLE player_performances ADD COLUMN riot_id_name VARCHAR(100)")
            print("[ETL] ✅ Columna añadida.")

    try:
        with get_mongo_client() as mongo_client:
            mongo_db = mongo_client[MONGO_DB]

            # Pool normal
            pool_id, l1_name = resolve_pool(mongo_db, args.pool, args.queue, args.min)
            populate(pool_id, l1_name, args.queue, args.min, mongo_db, pg_conn, args.users_collection)

            # Si no se especificó pool, también cargar season
            if not args.pool:
                season_l1 = f"L1_q{args.queue}_min{args.min}_pool_season"
                if season_l1 in mongo_db.list_collection_names():
                    populate("season", season_l1, args.queue, args.min, mongo_db, pg_conn, "L0_users_index_season")
    finally:
        pg_conn.close()

    print("[ETL] Finalizado.")


if __name__ == "__main__":
    main()
