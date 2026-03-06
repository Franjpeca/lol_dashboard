"""
dashboard/db.py
Capa de acceso a datos — toda query reside aquí.
Los módulos de páginas sólo llaman funciones de este fichero.
"""
import sys
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from utils.config import POSTGRES_URI

_PG_DSN = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")


# ─── Connection ──────────────────────────────────────────────────────────────

@st.cache_resource
def _conn():
    conn = psycopg2.connect(_PG_DSN)
    conn.autocommit = True
    return conn


@st.cache_data(ttl=300, show_spinner=False)
def _q(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = _conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return pd.DataFrame(cur.fetchall())


# ─── Meta ────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=600, show_spinner=False)
def get_pools() -> list[str]:
    df = _q("SELECT DISTINCT pool_id FROM pools ORDER BY pool_id")
    return df["pool_id"].tolist() if not df.empty else []


@st.cache_data(ttl=600, show_spinner=False)
def get_pool_options() -> dict[str, list[int]]:
    """
    Returns a dictionary mapping pool_id to a list of available min_friends.
    Example: {"season": [4, 5], "ca879f16": [5]}
    """
    df = _q("SELECT pool_id, min_friends FROM pools ORDER BY pool_id, min_friends")
    options = {}
    if not df.empty:
        for _, row in df.iterrows():
            pid = row["pool_id"]
            mf = row["min_friends"]
            if pid not in options:
                options[pid] = []
            options[pid].append(mf)
    return options


@st.cache_data(ttl=600, show_spinner=False)
def get_account_names() -> dict[str, str]:
    """Returns {puuid: 'RiotName#Tag'} from MongoDB."""
    try:
        from utils.db import get_mongo_client
        from utils.config import MONGO_DB, COLLECTION_USERS_INDEX
        mapping: dict[str, str] = {}
        with get_mongo_client() as client:
            db = client[MONGO_DB]
            for doc in db[COLLECTION_USERS_INDEX].find({}, {"accounts": 1}):
                for acc in doc.get("accounts", []):
                    puuid = acc.get("puuid")
                    riot_id = acc.get("riotId")
                    if puuid and riot_id:
                        mapping[puuid] = riot_id
        return mapping
    except Exception:
        return {}


# ─── Winrate / Partidas ──────────────────────────────────────────────────────

def get_community_overall_stats(pool_id: str, queue_id: int, min_friends: int) -> dict:
    df = _q("""
        WITH group_matches AS (
            SELECT m.match_id,
                   (SELECT win FROM player_performances pp
                    WHERE pp.match_id = m.match_id AND pp.is_friend = TRUE
                    LIMIT 1) AS group_win
            FROM matches m
            WHERE m.pool_id = %s AND m.queue_id = %s AND cardinality(m.friends_present) >= %s
        )
        SELECT COUNT(*) AS total_matches,
               SUM(CASE WHEN group_win THEN 1 ELSE 0 END) AS total_wins
        FROM group_matches
    """, (pool_id, queue_id, min_friends))
    if df.empty:
        return {"matches": 0, "winrate": 0}
    
    total = int(df.iloc[0]["total_matches"])
    wins = int(df.iloc[0]["total_wins"]) if df.iloc[0]["total_wins"] else 0
    wr = round((wins / total * 100), 1) if total > 0 else 0
    return {"matches": total, "winrate": wr}


@st.cache_data(ttl=300, show_spinner=False)
def get_top_outsider_allies(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    """
    Busca jugadores que NO son amigos (is_friend=FALSE) pero que han jugado
    en el MISMO equipo que los amigos en las partidas de esa pool.
    """
    return _q("""
        SELECT 
            riot_id_name as summoner,
            COUNT(*) as games,
            ROUND(AVG(CASE WHEN win THEN 1.0 ELSE 0.0 END) * 100, 1) as winrate
        FROM player_performances
        WHERE pool_id = %s 
          AND queue_id = %s 
          AND friends_count >= %s
          AND is_friend = FALSE
          AND riot_id_name IS NOT NULL
          AND riot_id_name <> 'Unknown'
          -- Solo aquellos que jugaron en el equipo que contenía a los amigos
          AND (match_id, pool_id, team_id) IN (
              SELECT match_id, pool_id, team_id 
              FROM player_performances 
              WHERE pool_id = %s AND is_friend = TRUE
          )
        GROUP BY riot_id_name
        ORDER BY games DESC, winrate DESC
    """, (pool_id, queue_id, min_friends, pool_id))


def get_winrate_by_persona(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    return _q("""
        SELECT persona, SUM(wins) AS wins, SUM(losses) AS losses, 
               SUM(total_matches) AS total_matches,
               ROUND((SUM(wins)::numeric / GREATEST(SUM(total_matches), 1)) * 100, 2) AS winrate
        FROM metric_01_players_winrate
        WHERE pool_id = %s AND queue_id = %s AND friends_count >= %s
        GROUP BY persona
        ORDER BY winrate DESC
    """, (pool_id, queue_id, min_friends))


def get_winrate_by_account(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    return _q("""
        SELECT
            puuid, persona,
            COUNT(*) AS total_matches,
            SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
            ROUND(SUM(CASE WHEN win THEN 1 ELSE 0 END)::numeric / GREATEST(COUNT(*), 1) * 100, 2) AS winrate
        FROM player_performances
        WHERE is_friend = TRUE AND pool_id = %s AND queue_id = %s AND friends_count >= %s
        GROUP BY puuid, persona
        ORDER BY winrate DESC
    """, (pool_id, queue_id, min_friends))


# ─── Estadísticas de Jugador ─────────────────────────────────────────────────

def get_player_stats(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    return _q("""
        SELECT persona, SUM(games) AS games, SUM(wins) AS wins,
               ROUND((SUM(wins)::numeric / GREATEST(SUM(games), 1)) * 100, 2) AS winrate,
               ROUND(AVG(avg_kills), 2) AS avg_kills,
               ROUND(AVG(avg_deaths), 2) AS avg_deaths,
               ROUND(AVG(avg_assists), 2) AS avg_assists,
               ROUND(AVG(avg_damage), 1) AS avg_damage,
               ROUND(AVG(avg_damage_taken), 1) AS avg_damage_taken,
               ROUND(AVG(avg_vision), 1) AS avg_vision,
               ROUND(AVG(avg_gold), 1) AS avg_gold,
               ROUND(AVG(avg_cs), 1) AS avg_cs
        FROM metric_05_player_stats
        WHERE pool_id = %s AND queue_id = %s AND friends_count >= %s
        GROUP BY persona
        ORDER BY persona
    """, (pool_id, queue_id, min_friends))


def get_player_performance_stats(pool_id: str, queue_id: int, min_friends: int, position: str = "Todos") -> pd.DataFrame:
    """
    Estadísticas completas por jugador con filtro de posición opcional.
    position: 'Todos' | 'TOP' | 'JUNGLE' | 'MID' | 'ADC' | 'SUPPORT'

    Lógica de detección de posición (basada en lane+role de Riot API):
      TOP     → lane='TOP'
      JUNGLE  → lane='JUNGLE'
      MID     → lane='MIDDLE'
      ADC     → lane='BOTTOM' AND role='CARRY'
      SUPPORT → role='SUPPORT'  (cubre BOTTOM+SUPPORT y NONE+SUPPORT)
    """
    _POS_FILTER = {
        "TOP":     "(p.role = 'TOP' OR (p.role IS NULL AND p.lane = 'TOP'))",
        "JUNGLE":  "(p.role = 'JUNGLE' OR (p.role IS NULL AND p.lane = 'JUNGLE'))",
        "MID":     "(p.role = 'MIDDLE' OR p.role = 'MID' OR (p.role IS NULL AND p.lane IN ('MIDDLE', 'MID')))",
        "ADC":     "(p.role = 'BOTTOM' OR (p.role IS NULL AND p.lane = 'BOTTOM' AND p.role = 'CARRY'))",
        "SUPPORT": "(p.role = 'UTILITY' OR p.role = 'SUPPORT' OR (p.role IS NULL AND p.lane = 'BOTTOM' AND p.role = 'SUPPORT'))",
    }
    pos_clause = _POS_FILTER.get(position, "TRUE")

    sql = f"""
        WITH team_kills AS (
            SELECT match_id, team_id, SUM(kills) AS total_team_kills
            FROM player_performances
            WHERE pool_id = %s AND queue_id = %s AND friends_count >= %s
            GROUP BY match_id, team_id
        )
        SELECT
            p.persona,
            COUNT(*) AS games,
            ROUND(AVG(p.damage_per_minute), 2)        AS avg_dmg_per_min,
            ROUND(AVG(p.gold_per_minute), 2)           AS avg_gold_per_min,
            ROUND(AVG(p.vision_score_per_minute), 2)   AS avg_vision_per_min,
            ROUND(AVG(
                (p.kills + p.assists)::numeric / GREATEST(p.deaths, 1)
            ), 2) AS avg_kda,
            ROUND(AVG(
                CASE WHEN tk.total_team_kills = 0 THEN 0
                ELSE (p.kills + p.assists)::numeric / tk.total_team_kills END
            ) * 100, 2) AS avg_kill_participation,
            ROUND(AVG(p.deaths), 2)                    AS avg_deaths,
            ROUND((SUM(CASE WHEN p.win THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) AS winrate
        FROM player_performances p
        JOIN team_kills tk ON p.match_id = tk.match_id AND p.team_id = tk.team_id
        WHERE p.is_friend = TRUE AND p.persona IS NOT NULL
          AND p.pool_id = %s AND p.queue_id = %s AND p.friends_count >= %s
          AND {pos_clause}
        GROUP BY p.persona
        ORDER BY p.persona
    """
    return _q(sql, (pool_id, queue_id, min_friends, pool_id, queue_id, min_friends))


def get_champion_stats_by_role(
    pool_id: str, queue_id: int, min_friends: int,
    persona: str = "Todos", position: str = "Todos"
) -> pd.DataFrame:
    """
    Campeones jugados filtrable por persona y posición.
    persona:  'Todos' o nombre concreto.
    position: 'Todos' | 'TOP' | 'JUNGLE' | 'MID' | 'ADC' | 'SUPPORT'
    """
    _POS_FILTER = {
        "TOP":     "(role = 'TOP' OR (role IS NULL AND lane = 'TOP'))",
        "JUNGLE":  "(role = 'JUNGLE' OR (role IS NULL AND lane = 'JUNGLE'))",
        "MID":     "(role = 'MIDDLE' OR role = 'MID' OR (role IS NULL AND lane IN ('MIDDLE', 'MID')))",
        "ADC":     "(role = 'BOTTOM' OR (role IS NULL AND lane = 'BOTTOM' AND role = 'CARRY'))",
        "SUPPORT": "(role = 'UTILITY' OR role = 'SUPPORT' OR (role IS NULL AND lane = 'BOTTOM' AND role = 'SUPPORT'))",
    }
    pos_clause     = _POS_FILTER.get(position, "TRUE")
    persona_clause = "persona = %s" if persona != "Todos" else "TRUE"
    params: tuple  = (pool_id, queue_id, min_friends)
    if persona != "Todos":
        params = (pool_id, queue_id, min_friends, persona)

    sql = f"""
        SELECT persona, champion_name,
               COUNT(*) AS total_matches,
               SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
               ROUND((SUM(CASE WHEN win THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) AS winrate
        FROM player_performances
        WHERE is_friend = TRUE AND persona IS NOT NULL
          AND pool_id = %s AND queue_id = %s AND friends_count >= %s
          AND {persona_clause}
          AND {pos_clause}
        GROUP BY persona, champion_name
        ORDER BY total_matches DESC
    """
    return _q(sql, params)


def get_champion_stats(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    return _q("""
        SELECT persona, champion_name, SUM(total_matches) AS total_matches, SUM(wins) AS wins,
               ROUND((SUM(wins)::numeric / GREATEST(SUM(total_matches), 1)) * 100, 2) AS winrate
        FROM metric_02_13_champions_winrate
        WHERE pool_id = %s AND queue_id = %s AND friends_count >= %s
        GROUP BY persona, champion_name
        ORDER BY persona, total_matches DESC
    """, (pool_id, queue_id, min_friends))


def get_community_champions(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    return _q("""
        SELECT champion_name, SUM(games) AS games, SUM(wins) AS wins,
               ROUND((SUM(wins)::numeric / GREATEST(SUM(games), 1)) * 100, 2) AS winrate
        FROM metric_14_community_champions
        WHERE pool_id = %s AND queue_id = %s AND friends_count >= %s
        GROUP BY champion_name
        ORDER BY games DESC
    """, (pool_id, queue_id, min_friends))


def get_enemy_champions(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    return _q("""
        SELECT champion_name, SUM(games) AS games, SUM(wins) AS wins,
               ROUND((SUM(wins)::numeric / GREATEST(SUM(games), 1)) * 100, 2) AS winrate
        FROM metric_15_enemy_champions
        WHERE pool_id = %s AND queue_id = %s AND friends_count >= %s
        GROUP BY champion_name
        ORDER BY games DESC
    """, (pool_id, queue_id, min_friends))


# ─── Índices ─────────────────────────────────────────────────────────────────

def get_ego_index(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    """Alias → delega en get_ego_score que implementa el cálculo completo con friends_count."""
    return get_ego_score(pool_id, queue_id, min_friends)


def get_troll_index(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    return _q("""
        SELECT persona, SUM(games) AS games, SUM(early_surrenders) AS early_surrenders, SUM(afks) AS afks
        FROM metric_07_troll_index
        WHERE pool_id = %s AND queue_id = %s AND friends_count >= %s
        GROUP BY persona
        ORDER BY (SUM(early_surrenders) + SUM(afks)) DESC
    """, (pool_id, queue_id, min_friends))


def get_first_metrics(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    return _q("""
        SELECT persona, SUM(games) AS games,
               SUM(total_fb_kills) AS total_fb_kills, SUM(total_fb_assists) AS total_fb_assists,
               ROUND(AVG(avg_early_takedowns), 2) AS avg_early_takedowns, 
               ROUND(AVG(avg_early_gold_per_min), 2) AS avg_early_gold_per_min,
               ROUND(AVG(avg_early_dmg_per_min), 2) AS avg_early_dmg_per_min, 
               ROUND(AVG(avg_early_vision_per_min), 2) AS avg_early_vision_per_min, 
               ROUND(AVG(avg_early_cs_10m), 2) AS avg_early_cs_10m
        FROM metric_08_first_metrics
        WHERE pool_id = %s AND queue_id = %s AND friends_count >= %s
        GROUP BY persona
        ORDER BY persona
    """, (pool_id, queue_id, min_friends))


def get_skills(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    return _q("""
        SELECT persona,
               SUM(games) AS games,
               ROUND(AVG(avg_q_casts), 1) AS avg_q_casts,
               ROUND(AVG(avg_w_casts), 1) AS avg_w_casts,
               ROUND(AVG(avg_e_casts), 1) AS avg_e_casts,
               ROUND(AVG(avg_r_casts), 1) AS avg_r_casts,
               MAX(max_q_casts) AS max_q_casts,
               MAX(max_w_casts) AS max_w_casts,
               MAX(max_e_casts) AS max_e_casts,
               MAX(max_r_casts) AS max_r_casts
        FROM metric_09_skills
        WHERE pool_id = %s AND queue_id = %s AND friends_count >= %s
        GROUP BY persona
        ORDER BY persona
    """, (pool_id, queue_id, min_friends))


# ─── Estadísticas por Rol ────────────────────────────────────────────────────

def get_stats_by_role(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    return _q("""
        SELECT persona, position, SUM(games) AS games, SUM(wins) AS wins,
               ROUND((SUM(wins)::numeric / GREATEST(SUM(games), 1)) * 100, 2) AS winrate,
               ROUND(AVG(avg_kills), 2) AS avg_kills,
               ROUND(AVG(avg_deaths), 2) AS avg_deaths,
               ROUND(AVG(avg_assists), 2) AS avg_assists,
               ROUND(AVG(avg_damage), 1) AS avg_damage,
               ROUND(AVG(avg_damage_taken), 1) AS avg_damage_taken,
               ROUND(AVG(avg_vision), 1) AS avg_vision,
               ROUND(AVG(avg_gold), 1) AS avg_gold,
               ROUND(AVG(avg_cs), 1) AS avg_cs
        FROM metric_10_stats_by_role
        WHERE pool_id = %s AND queue_id = %s AND friends_count >= %s
        GROUP BY persona, position
        ORDER BY persona, games DESC
    """, (pool_id, queue_id, min_friends))


# ─── Récords ─────────────────────────────────────────────────────────────────

def get_records(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    return _q("""
        SELECT persona,
               MAX(max_kills) AS max_kills, MAX(max_deaths) AS max_deaths, MAX(max_assists) AS max_assists,
               MAX(max_vision_score) AS max_vision_score, MAX(max_cs) AS max_cs, 
               MAX(max_damage_dealt) AS max_damage_dealt,
               MAX(max_gold) AS max_gold, MAX(max_duration_s) AS max_duration_s
        FROM metric_11_records
        WHERE pool_id = %s AND queue_id = %s AND friends_count >= %s
        GROUP BY persona
        ORDER BY persona
    """, (pool_id, queue_id, min_friends))


def get_streaks(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    # Nota: Las rachas dependen del orden temporal. Aquí usamos player_performances real-time 
    # para asegurar que si el usuario filtra por min_friends, la racha es sobre esa serie.
    return _q("""
        WITH ordered AS (
            SELECT persona, win, 
                   ROW_NUMBER() OVER(PARTITION BY persona ORDER BY game_start_at) AS rn,
                   ROW_NUMBER() OVER(PARTITION BY persona, win ORDER BY game_start_at) AS rn_win
            FROM player_performances
            WHERE is_friend = TRUE AND persona IS NOT NULL
              AND pool_id = %s AND queue_id = %s AND friends_count >= %s
        ),
        streak_groups AS (
            SELECT persona, win, (rn - rn_win) AS grp, COUNT(*) AS streak_len
            FROM ordered
            GROUP BY persona, win, grp
        )
        SELECT persona, 
               MAX(CASE WHEN win THEN streak_len ELSE 0 END) AS max_win_streak,
               MAX(CASE WHEN NOT win THEN streak_len ELSE 0 END) AS max_lose_streak
        FROM streak_groups
        GROUP BY persona
        ORDER BY max_win_streak DESC
    """, (pool_id, queue_id, min_friends))


_POS_FILTER_REC = {
    "TOP":     "(role = 'TOP' OR (role IS NULL AND lane = 'TOP'))",
    "JUNGLE":  "(role = 'JUNGLE' OR (role IS NULL AND lane = 'JUNGLE'))",
    "MID":     "(role = 'MIDDLE' OR role = 'MID' OR (role IS NULL AND lane IN ('MIDDLE', 'MID')))",
    "ADC":     "(role = 'BOTTOM' OR (role IS NULL AND lane = 'BOTTOM' AND role = 'CARRY'))",
    "SUPPORT": "(role = 'UTILITY' OR role = 'SUPPORT' OR (role IS NULL AND lane = 'BOTTOM' AND role = 'SUPPORT'))",
}


def get_records_by_stat(pool_id: str, queue_id: int, min_friends: int, stat_col: str, position: str = "Todos") -> pd.DataFrame:
    """
    Récords de cada jugador filtrados por posición y por una estadística específica.
    Devuelve la partida exacta (`record_match_id`) del récord.
    """
    pos_clause = _POS_FILTER_REC.get(position, "TRUE")
    
    valid_cols = {"kills", "deaths", "assists", "vision_score", "cs_total", "damage_dealt", "gold_earned"}
    col = stat_col if stat_col in valid_cols else "kills"

    return _q(f"""
        SELECT DISTINCT ON (persona)
               persona,
               {col} AS record_value,
               match_id AS record_match_id
        FROM player_performances
        WHERE is_friend = TRUE AND persona IS NOT NULL
          AND pool_id = %s AND queue_id = %s AND friends_count >= %s
          AND {pos_clause}
        ORDER BY persona, {col} DESC, game_start_at DESC
    """, (pool_id, queue_id, min_friends))


def get_streaks_by_role(pool_id: str, queue_id: int, min_friends: int, position: str = "Todos") -> pd.DataFrame:
    """Rachas máximas de victorias/derrotas por jugador, filtradas por posición."""
    pos_clause = _POS_FILTER_REC.get(position, "TRUE")
    return _q(f"""
        WITH ordered AS (
            SELECT
                persona,
                win,
                ROW_NUMBER() OVER (PARTITION BY persona ORDER BY game_start_at) AS rn
            FROM player_performances
            WHERE is_friend = TRUE AND persona IS NOT NULL
              AND pool_id = %s AND queue_id = %s AND friends_count >= %s
              AND {pos_clause}
        ),
        grouped AS (
            SELECT
                persona,
                win,
                rn - ROW_NUMBER() OVER (PARTITION BY persona, win ORDER BY rn) AS grp
            FROM ordered
        ),
        streaks AS (
            SELECT persona, win, COUNT(*) AS streak_len
            FROM grouped
            GROUP BY persona, win, grp
        )
        SELECT
            persona,
            MAX(CASE WHEN win THEN streak_len ELSE 0 END) AS max_win_streak,
            MAX(CASE WHEN NOT win THEN streak_len ELSE 0 END) AS max_lose_streak
        FROM streaks
        GROUP BY persona
        ORDER BY max_win_streak DESC
    """, (pool_id, queue_id, min_friends))



# ─── Índice de Ego ────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner=False)
def get_ego_score(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    """
    Índice de Ego por persona.
    ego = (damage_share + gold_share) - assist_share
    damage_share / gold_share / assist_share -> % del amigo respecto a su equipo real de 5.
    """
    return _q("""
        WITH
        -- 1. Amigos en partidas del pool/queue
        friends AS (
            SELECT
                persona, match_id, team_id,
                damage_dealt, gold_earned, kills, assists,
                damage_per_minute, gold_per_minute
            FROM player_performances
            WHERE is_friend = TRUE
              AND persona IS NOT NULL
              AND pool_id = %s AND queue_id = %s AND friends_count >= %s
        ),
        -- 2. Totales del equipo completo (los 5) para los matches del pool
        team_totals AS (
            SELECT
                t.match_id,
                t.team_id,
                SUM(t.damage_dealt) AS team_damage,
                SUM(t.gold_earned)  AS team_gold,
                SUM(t.kills)        AS team_kills,
                SUM(t.assists)      AS team_assists
            FROM player_performances t
            WHERE t.pool_id = %s AND t.queue_id = %s AND t.friends_count >= %s
            GROUP BY t.match_id, t.team_id
        ),
        -- 3. Shares por partida
        shares AS (
            SELECT
                f.persona,
                f.match_id,
                CASE WHEN tt.team_damage > 0 THEN f.damage_dealt * 100.0 / tt.team_damage ELSE 0 END AS damage_share,
                CASE WHEN tt.team_gold   > 0 THEN f.gold_earned  * 100.0 / tt.team_gold   ELSE 0 END AS gold_share,
                CASE WHEN tt.team_assists > 0 THEN f.assists * 100.0 / tt.team_assists ELSE 0 END AS assist_share,
                CASE WHEN tt.team_kills > 0 THEN (f.kills + f.assists) * 100.0 / tt.team_kills ELSE 0 END AS kp,
                f.damage_per_minute AS dpm,
                f.gold_per_minute   AS gpm
            FROM friends f
            JOIN team_totals tt ON f.match_id = tt.match_id AND f.team_id = tt.team_id
        )
        SELECT
            persona,
            ROUND(AVG(damage_share)::numeric, 1) AS avg_damage_share,
            ROUND(AVG(gold_share)::numeric, 1)   AS avg_gold_share,
            ROUND(AVG(assist_share)::numeric, 1) AS avg_assist_share,
            ROUND(AVG(kp)::numeric, 1)           AS avg_kp,
            ROUND(AVG(damage_share + gold_share - assist_share)::numeric, 2) AS ego_score,
            ROUND(AVG(dpm)::numeric, 1)          AS avg_dpm,
            ROUND(AVG(gpm)::numeric, 1)          AS avg_gpm,
            COUNT(*)                              AS partidas
        FROM shares
        GROUP BY persona
        ORDER BY ego_score DESC
    """, (pool_id, queue_id, min_friends, pool_id, queue_id, min_friends))


@st.cache_data(ttl=300, show_spinner=False)
def get_troll_index(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    """
    Troll Index por persona.
    Detecta partidas AFK, griefing e intentional feeding usando heurísticas.
    troll_index = 0.5 * afk_rate + 0.3 * grief_rate + 0.2 * remake_rate
    """
    return _q("""
        WITH
        -- Totales de equipo para KP y damage share
        team_totals AS (
            SELECT
                t.match_id, t.team_id,
                SUM(t.kills)        AS team_kills,
                SUM(t.damage_dealt) AS team_damage
            FROM player_performances t
            WHERE t.pool_id = %s AND t.queue_id = %s AND t.friends_count >= %s
            GROUP BY t.match_id, t.team_id
        ),
        base AS (
            SELECT
                pp.persona,
                pp.match_id,
                pp.kills, pp.deaths, pp.assists,
                pp.damage_per_minute,
                pp.gold_per_minute,
                pp.cs_total,
                pp.duration_s,
                tt.team_kills,
                tt.team_damage,
                pp.damage_dealt,
                -- AFK heuristic: DPM muy bajo, GPM muy bajo Y CS muy bajo
                CASE WHEN pp.damage_per_minute < 50
                      AND pp.gold_per_minute < 100
                      AND pp.cs_total < 20
                    THEN 1 ELSE 0 END AS is_afk,
                -- Grief/int heuristic: muertes altas, KP baja, daño bajo
                CASE WHEN pp.deaths >= 10
                      AND (CASE WHEN tt.team_kills > 0
                                THEN (pp.kills + pp.assists) * 100.0 / tt.team_kills
                                ELSE 0 END) < 20
                      AND (CASE WHEN tt.team_damage > 0
                                THEN pp.damage_dealt * 100.0 / tt.team_damage
                                ELSE 0 END) < 10
                    THEN 1 ELSE 0 END AS is_grief,
                -- Remake: partida menor de 5 minutos
                CASE WHEN pp.duration_s < 300 THEN 1 ELSE 0 END AS is_remake
            FROM player_performances pp
            JOIN team_totals tt ON pp.match_id = tt.match_id AND pp.team_id = tt.team_id
            WHERE pp.is_friend = TRUE
              AND pp.persona IS NOT NULL
              AND pp.pool_id = %s AND pp.queue_id = %s AND pp.friends_count >= %s
        )
        SELECT
            persona,
            COUNT(*)                                                    AS total_games,
            SUM(is_afk)                                                 AS afk_games,
            SUM(is_grief)                                               AS grief_games,
            SUM(is_remake)                                              AS remake_games,
            ROUND((SUM(is_afk)::numeric    / COUNT(*)) * 100, 1)       AS afk_rate,
            ROUND((SUM(is_grief)::numeric  / COUNT(*)) * 100, 1)       AS grief_rate,
            ROUND((SUM(is_remake)::numeric / COUNT(*)) * 100, 1)       AS remake_rate,
            ROUND((
                0.5 * (SUM(is_afk)::numeric    / COUNT(*)) +
                0.3 * (SUM(is_grief)::numeric  / COUNT(*)) +
                0.2 * (SUM(is_remake)::numeric / COUNT(*))
            ) * 100, 2) AS troll_index
        FROM base
        GROUP BY persona
        ORDER BY troll_index DESC
    """, (pool_id, queue_id, min_friends, pool_id, queue_id, min_friends))



# ─── Sinergia ────────────────────────────────────────────────────────────────

def get_botlane_synergy(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    return _q("""
        SELECT p1, p2, SUM(games) AS games, SUM(wins) AS wins,
               ROUND((SUM(wins)::numeric / GREATEST(SUM(games), 1)) * 100, 2) AS winrate,
               ROUND(AVG(avg_combined_kda), 2) AS avg_combined_kda,
               ROUND(AVG(avg_combined_damage), 1) AS avg_combined_damage,
               ROUND(AVG(avg_combined_gold), 1) AS avg_combined_gold,
               ROUND(AVG(avg_combined_vision), 1) AS avg_combined_vision
        FROM metric_12_botlane_synergy
        WHERE pool_id = %s AND queue_id = %s AND friends_count >= %s
        GROUP BY p1, p2
        ORDER BY winrate DESC
    """, (pool_id, queue_id, min_friends))


# ─── Últimas partidas ────────────────────────────────────────────────────────

def get_recent_matches(pool_id: str, queue_id: int, min_friends: int, limit: int = 20) -> pd.DataFrame:
    """Alias de compatibilidad sin filtros avanzados."""
    return get_matches_filtered(pool_id, queue_id, min_friends, limit=limit)


@st.cache_data(ttl=300, show_spinner=False)
def get_all_personas(pool_id: str, queue_id: int, min_friends: int) -> list[str]:
    """Devuelve la lista de alias únicos del grupo (para el filtro de persona)."""
    df = _q("""
        SELECT DISTINCT persona FROM player_performances
        WHERE is_friend = TRUE AND persona IS NOT NULL
          AND pool_id = %s AND queue_id = %s AND friends_count >= %s
        ORDER BY persona
    """, (pool_id, queue_id, min_friends))
    return df["persona"].tolist() if not df.empty else []


@st.cache_data(ttl=300, show_spinner=False)
def get_matches_filtered(
    pool_id: str,
    queue_id: int,
    min_friends: int,
    limit: int = 20,
    match_id_search: str = "",
    date_filter: str = "",
    persona_filter: str = "",
) -> pd.DataFrame:
    """
    Devuelve el listado de partidas con filtros opcionales.
    - match_id_search : filtra por ID (búsqueda parcial)
    - date_filter     : 'YYYY-MM-DD' para un día concreto
    - persona_filter  : alias exacto que debe estar en personas_present
    """
    conditions = ["m.pool_id = %s", "m.queue_id = %s"]
    params = [pool_id, queue_id]

    if match_id_search:
        conditions.append("m.match_id ILIKE %s")
        params.append(f"%{match_id_search}%")

    if date_filter:
        conditions.append("DATE(m.game_start_at) = %s")
        params.append(date_filter)

    if persona_filter:
        conditions.append("%s = ANY(m.personas_present)")
        params.append(persona_filter)

    conditions.append("cardinality(m.friends_present) >= %s")
    params.append(min_friends)

    where_clause = " AND ".join(conditions)
    params.append(limit)

    return _q(f"""
        SELECT m.match_id, m.game_start_at, m.duration_s,
               m.friends_present, m.personas_present, m.winning_team,
               (SELECT win FROM player_performances pp
                WHERE pp.match_id = m.match_id AND pp.is_friend = TRUE
                LIMIT 1) AS group_win
        FROM matches m
        WHERE {where_clause}
        ORDER BY m.game_start_at DESC
        LIMIT %s
    """, tuple(params))



# ─── Frecuencia de Partidas ──────────────────────────────────────────────────

def get_matches_per_day(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    """Número total de partidas por día (comunidad completa)."""
    return _q("""
        SELECT DATE(game_start_at) AS day, COUNT(*) AS matches
        FROM matches
        WHERE pool_id = %s AND queue_id = %s AND cardinality(friends_present) >= %s
        GROUP BY DATE(game_start_at)
        ORDER BY day
    """, (pool_id, queue_id, min_friends))


def get_matches_per_day_persona(pool_id: str, queue_id: int, min_friends: int) -> pd.DataFrame:
    """Número de partidas por día y persona."""
    return _q("""
        SELECT pp.persona,
               DATE(pp.game_start_at) AS day,
               COUNT(*) AS matches
        FROM player_performances pp
        WHERE pp.is_friend = TRUE
          AND pp.pool_id = %s
          AND pp.queue_id = %s
          AND pp.friends_count >= %s
        GROUP BY pp.persona, DATE(pp.game_start_at)
        ORDER BY pp.persona, day
    """, (pool_id, queue_id, min_friends))


# ─── Detalle de partida (MongoDB raw) ────────────────────────────────────────

@st.cache_data(ttl=600, show_spinner=False)
def get_match_detail(match_id: str) -> dict:
    """
    Carga el detalle completo de una partida desde MongoDB (L0) y lo devuelve
    formateado listo para renderizar. Misma lógica que example/viewGame/loader.py.
    """
    try:
        from utils.db import get_mongo_client
        from utils.config import MONGO_DB, COLLECTION_RAW_MATCHES, COLLECTION_USERS_INDEX
        from datetime import datetime

        with get_mongo_client() as client:
            mongo_db = client[MONGO_DB]

            # ── Mapa puuid → persona ──────────────────────────────────────
            puuid_to_name: dict[str, str] = {}
            for doc in mongo_db[COLLECTION_USERS_INDEX].find({}, {"puuids": 1, "riotIds": 1, "accounts": 1}):
                # Formato nuevo: accounts[]
                for acc in doc.get("accounts", []):
                    p = acc.get("puuid")
                    riot_id = acc.get("riotId", "")
                    if p and riot_id:
                        puuid_to_name[p] = riot_id
                # Formato antiguo: puuids[] + riotIds[]
                puuids = doc.get("puuids", [])
                riot_ids = doc.get("riotIds", [])
                name = riot_ids[-1] if riot_ids else None
                for p in puuids:
                    if p not in puuid_to_name and name:
                        puuid_to_name[p] = name

            # ── Documento de la partida ───────────────────────────────────
            doc = mongo_db[COLLECTION_RAW_MATCHES].find_one({"_id": match_id})
            if not doc:
                return {"error": f"Partida {match_id} no encontrada en MongoDB"}

            data = doc.get("data", {})
            info = data.get("info")
            if not info:
                return {"error": "Partida corrupta o incompleta"}

            # Fallback desde PostgreSQL (cuando en Mongo falte/sea 0 el daño)
            pg_damage_by_puuid: dict[str, int] = {}
            try:
                pg_df = _q(
                    "SELECT puuid, damage_dealt FROM player_performances WHERE match_id=%s",
                    (match_id,)
                )
                if not pg_df.empty:
                    for _, r in pg_df.iterrows():
                        pu = r.get("puuid")
                        dmg_pg = r.get("damage_dealt")
                        if pu and dmg_pg is not None:
                            try:
                                pg_damage_by_puuid[pu] = int(dmg_pg)
                            except Exception:
                                pass
            except Exception:
                # Si PG no está disponible, seguimos con datos de Mongo
                pg_damage_by_puuid = {}

            duration_s = info.get("gameDuration", 0)
            duration_min = max(1, duration_s / 60)
            ts = info.get("gameStartTimestamp")
            start_time = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d %H:%M") if ts else "-"

            # ── Builder de jugador ────────────────────────────────────────
            def build_player(p):
                puuid = p.get("puuid", "")
                riot_id = ""
                if p.get("riotIdGameName") and p.get("riotIdTagLine"):
                    riot_id = f"{p['riotIdGameName']}#{p['riotIdTagLine']}"
                elif p.get("riotIdGameName"):
                    riot_id = p["riotIdGameName"]
                fallback_name = riot_id or p.get("summonerName") or puuid[:8]
                name = puuid_to_name.get(puuid, fallback_name)
                farm = p.get("totalMinionsKilled", 0) + p.get("neutralMinionsKilled", 0)

                items_raw = p.get("items")
                if isinstance(items_raw, list) and len(items_raw) == 7:
                    items = items_raw
                else:
                    items = [p.get(f"item{i}", 0) for i in range(7)]

                perks = p.get("perks", {}).get("styles", [])
                primary = perks[0].get("selections", [{}])[0].get("perk") if len(perks) >= 1 else None
                secondary = perks[1].get("style") if len(perks) >= 2 else None

                # Daño: algunos matches pueden venir sin totalDamageDealtToChampions
                # (o a 0) según fuente/parche. Aplicamos fallback robusto.
                dmg = p.get("totalDamageDealtToChampions")
                if dmg is None:
                    dmg = 0

                if dmg == 0:
                    # Fallback 1: suma de daño físico/mágico/verdadero a campeones
                    phys = p.get("physicalDamageDealtToChampions") or 0
                    magic = p.get("magicDamageDealtToChampions") or 0
                    true_dmg = p.get("trueDamageDealtToChampions") or 0
                    sum_parts = phys + magic + true_dmg
                    if sum_parts > 0:
                        dmg = sum_parts

                if dmg == 0:
                    # Fallback 2: estimación por challenges.damagePerMinute
                    ch = p.get("challenges") or {}
                    dpm_ch = ch.get("damagePerMinute")
                    if dpm_ch:
                        try:
                            dmg = int(round(float(dpm_ch) * duration_min))
                        except Exception:
                            pass

                if dmg == 0:
                    # Fallback 3: daño desde PostgreSQL por puuid (si existe)
                    dmg_pg = pg_damage_by_puuid.get(puuid)
                    if isinstance(dmg_pg, int) and dmg_pg > 0:
                        dmg = dmg_pg

                gold = p.get("goldEarned", 0)

                return {
                    "puuid":        puuid,
                    "name":         name,
                    "champ":        p.get("championName", ""),
                    "champLevel":   p.get("champLevel", 1),
                    "kills":        p.get("kills", 0),
                    "deaths":       p.get("deaths", 0),
                    "assists":      p.get("assists", 0),
                    "kda":          round((p.get("kills", 0) + p.get("assists", 0)) / max(1, p.get("deaths", 0)), 2),
                    "damage":       dmg,
                    "dpm":          round(dmg / duration_min, 1),
                    "gold":         gold,
                    "gpm":          round(gold / duration_min, 1),
                    "cs":           farm,
                    "cspm":         round(farm / duration_min, 1),
                    "visionScore":  p.get("visionScore", 0),
                    "summoner1Id":  p.get("summoner1Id"),
                    "summoner2Id":  p.get("summoner2Id"),
                    "primary":      primary,
                    "secondary":    secondary,
                    "items":        items,
                    "role":         p.get("teamPosition", ""),
                    "win":          p.get("win", False),
                }

            blue_players, red_players = [], []
            for p in info.get("participants", []):
                entry = build_player(p)
                (blue_players if p.get("teamId") == 100 else red_players).append(entry)

            # Damage share
            for players in (blue_players, red_players):
                total = sum(pl["damage"] for pl in players) or 1
                for pl in players:
                    pl["damageShare"] = round(pl["damage"] / total * 100, 1)

            teams_raw = info.get("teams", [])
            blue_win = next((t["win"] for t in teams_raw if t["teamId"] == 100), False)
            red_win  = next((t["win"] for t in teams_raw if t["teamId"] == 200), False)

            return {
                "matchId":    match_id,
                "queueId":    info.get("queueId"),
                "duration":   duration_s,
                "start_time": start_time,
                "teams": {
                    "blue": {"win": blue_win, "players": blue_players},
                    "red":  {"win": red_win,  "players": red_players},
                },
            }

    except Exception as exc:
        return {"error": str(exc)}


