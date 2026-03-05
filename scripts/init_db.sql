-- LoL Analytics — PostgreSQL Schema
-- Ejecutar con: python scripts/apply_schema.py
-- O directamente: psql -U lol_user -d lol_analytics -f scripts/init_db.sql

-- ======================================================
-- DIMENSIÓN: pools
-- ======================================================
CREATE TABLE IF NOT EXISTS pools (
    pool_id     VARCHAR(30)  NOT NULL,
    min_friends INTEGER      NOT NULL DEFAULT 5,
    personas    TEXT[]       NOT NULL DEFAULT '{}',
    queue_id    INTEGER      NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pool_id, min_friends)
);

-- ======================================================
-- L1: matches
-- Una fila por partida filtrada. Equivale a L1_q*_min*_pool_*
-- ======================================================
CREATE TABLE IF NOT EXISTS matches (
    match_id          VARCHAR(30)  NOT NULL,
    pool_id           VARCHAR(30)  REFERENCES pools(pool_id) ON DELETE CASCADE,
    queue_id          INTEGER      NOT NULL,
    min_friends       INTEGER      NOT NULL,
    duration_s        INTEGER,
    game_start_ts     BIGINT,                 -- timestamp ms (Riot raw)
    game_start_at     TIMESTAMPTZ,
    game_end_at       TIMESTAMPTZ,
    friends_present   TEXT[]       NOT NULL DEFAULT '{}',
    personas_present  TEXT[]       NOT NULL DEFAULT '{}',
    winning_team      INTEGER,                -- 100 o 200
    filtered_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (match_id, pool_id)
);

CREATE INDEX IF NOT EXISTS idx_matches_pool_queue ON matches (pool_id, queue_id);
CREATE INDEX IF NOT EXISTS idx_matches_start     ON matches (game_start_at);
CREATE INDEX IF NOT EXISTS idx_matches_pool_start ON matches (pool_id, game_start_at);

-- ======================================================
-- L2: player_performances
-- Una fila por jugador x partida. Fusiona L2_players_flat y L2_enemies_flat
-- ======================================================
CREATE TABLE IF NOT EXISTS player_performances (
    id              BIGSERIAL    PRIMARY KEY,
    match_id        VARCHAR(30)  NOT NULL,
    puuid           VARCHAR(100) NOT NULL,
    persona         VARCHAR(100),             -- NULL si es rival
    is_friend       BOOLEAN      NOT NULL,
    champion_name   VARCHAR(60),
    team_id         INTEGER,                  -- 100 o 200
    win             BOOLEAN,
    lane            VARCHAR(20),
    role            VARCHAR(20),
    kills           INTEGER,
    deaths          INTEGER,
    assists         INTEGER,
    gold_earned     INTEGER,
    damage_dealt    INTEGER,
    damage_taken    INTEGER,
    vision_score    INTEGER,
    damage_mitigated INTEGER,
    cs_total        INTEGER,                  -- totalMinionsKilled + neutralMinionsKilled
    game_ended_surrender BOOLEAN DEFAULT FALSE,
    pool_id         VARCHAR(30),
    queue_id        INTEGER,
    game_start_at   TIMESTAMPTZ,
    duration_s      INTEGER,
    friends_count   INTEGER,                  -- Número real de amigos en esta partida

    -- Metrics 08 & 09 fields
    first_blood_kill        BOOLEAN DEFAULT FALSE,
    first_blood_assist      BOOLEAN DEFAULT FALSE,
    longest_time_spent_living INTEGER,
    takedowns_first_x_minutes NUMERIC,
    gold_per_minute         NUMERIC,
    damage_per_minute       NUMERIC,
    vision_score_per_minute   NUMERIC,
    lane_minions_first_10_minutes INTEGER,
    spell1_casts            INTEGER DEFAULT 0,
    spell2_casts            INTEGER DEFAULT 0,
    spell3_casts            INTEGER DEFAULT 0,
    spell4_casts            INTEGER DEFAULT 0,

    UNIQUE (match_id, pool_id, puuid)
);

CREATE INDEX IF NOT EXISTS idx_pp_puuid_pool   ON player_performances (puuid, pool_id);
CREATE INDEX IF NOT EXISTS idx_pp_persona_pool ON player_performances (persona, pool_id);
CREATE INDEX IF NOT EXISTS idx_pp_match        ON player_performances (match_id);
CREATE INDEX IF NOT EXISTS idx_pp_champion     ON player_performances (champion_name);
CREATE INDEX IF NOT EXISTS idx_pp_start        ON player_performances (game_start_at);
CREATE INDEX IF NOT EXISTS idx_pp_friend       ON player_performances (is_friend, pool_id);
CREATE INDEX IF NOT EXISTS idx_pp_lane         ON player_performances (lane, pool_id);
