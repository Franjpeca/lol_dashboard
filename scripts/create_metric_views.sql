-- scripts/create_metric_views.sql
-- Métricas en tiempo real sobre PostgreSQL mediante Vistas.
-- Estas vistas incluyen friends_count para permitir filtrado dinámico en el dashboard.

DROP VIEW IF EXISTS metric_01_players_winrate CASCADE;
DROP VIEW IF EXISTS metric_02_13_champions_winrate CASCADE;
DROP VIEW IF EXISTS metric_05_player_stats CASCADE;
DROP VIEW IF EXISTS metric_07_troll_index CASCADE;
DROP VIEW IF EXISTS metric_08_first_metrics CASCADE;
DROP VIEW IF EXISTS metric_10_stats_by_role CASCADE;
DROP VIEW IF EXISTS metric_11_records CASCADE;
DROP VIEW IF EXISTS metric_12_botlane_synergy CASCADE;
DROP VIEW IF EXISTS metric_14_community_champions CASCADE;
DROP VIEW IF EXISTS metric_15_enemy_champions CASCADE;

-- 1. Winrates por jugador
CREATE VIEW metric_01_players_winrate AS
SELECT pool_id, queue_id, friends_count, persona,
       COUNT(*) AS total_matches,
       SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
       SUM(CASE WHEN NOT win THEN 1 ELSE 0 END) AS losses,
       ROUND((SUM(CASE WHEN win THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) AS winrate
FROM player_performances
WHERE is_friend = TRUE AND persona IS NOT NULL
GROUP BY pool_id, queue_id, friends_count, persona;

-- 2 & 13. Winrates por campeón y jugador
CREATE VIEW metric_02_13_champions_winrate AS
SELECT pool_id, queue_id, friends_count, persona, champion_name,
       COUNT(*) AS total_matches,
       SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
       SUM(CASE WHEN NOT win THEN 1 ELSE 0 END) AS losses,
       ROUND((SUM(CASE WHEN win THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) AS winrate
FROM player_performances
WHERE is_friend = TRUE AND persona IS NOT NULL
GROUP BY pool_id, queue_id, friends_count, persona, champion_name;

-- 5. Estadísticas promedio por jugador
CREATE VIEW metric_05_player_stats AS
SELECT pool_id, queue_id, friends_count, persona,
       COUNT(*) AS games,
       SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
       ROUND((SUM(CASE WHEN win THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) AS winrate,
       ROUND(AVG(kills), 2) AS avg_kills,
       ROUND(AVG(deaths), 2) AS avg_deaths,
       ROUND(AVG(assists), 2) AS avg_assists,
       ROUND(AVG(damage_dealt), 2) AS avg_damage,
       ROUND(AVG(damage_taken), 2) AS avg_damage_taken,
       ROUND(AVG(vision_score), 2) AS avg_vision,
       ROUND(AVG(gold_earned), 2) AS avg_gold,
       ROUND(AVG(cs_total), 2) AS avg_cs
FROM player_performances
WHERE is_friend = TRUE AND persona IS NOT NULL
GROUP BY pool_id, queue_id, friends_count, persona;

-- 7. Troll Index
CREATE VIEW metric_07_troll_index AS
SELECT pool_id, queue_id, friends_count, persona,
       COUNT(*) AS games,
       SUM(CASE WHEN game_ended_surrender AND NOT win THEN 1 ELSE 0 END) AS early_surrenders,
       SUM(CASE WHEN cs_total < 10 AND (kills+assists) = 0 AND duration_s > 600 THEN 1 ELSE 0 END) AS afks
FROM player_performances
WHERE is_friend = TRUE AND persona IS NOT NULL
GROUP BY pool_id, queue_id, friends_count, persona;

-- 8. First Metrics e Inicio de partida
CREATE VIEW metric_08_first_metrics AS
SELECT pool_id, queue_id, friends_count, persona,
       COUNT(*) AS games,
       SUM(CASE WHEN first_blood_kill THEN 1 ELSE 0 END) AS total_fb_kills,
       SUM(CASE WHEN first_blood_assist THEN 1 ELSE 0 END) AS total_fb_assists,
       ROUND(AVG(takedowns_first_x_minutes), 2) AS avg_early_takedowns,
       ROUND(AVG(gold_per_minute), 2) AS avg_early_gold_per_min,
       ROUND(AVG(damage_per_minute), 2) AS avg_early_dmg_per_min,
       ROUND(AVG(vision_score_per_minute), 2) AS avg_early_vision_per_min,
       ROUND(AVG(lane_minions_first_10_minutes), 2) AS avg_early_cs_10m
FROM player_performances
WHERE is_friend = TRUE AND persona IS NOT NULL
GROUP BY pool_id, queue_id, friends_count, persona;

-- 10. Stats by Role
CREATE VIEW metric_10_stats_by_role AS
SELECT pool_id, queue_id, friends_count, persona, COALESCE(role, lane, 'UNKNOWN') AS position,
       COUNT(*) AS games,
       SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
       ROUND((SUM(CASE WHEN win THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) AS winrate,
       ROUND(AVG(kills), 2) AS avg_kills,
       ROUND(AVG(deaths), 2) AS avg_deaths,
       ROUND(AVG(assists), 2) AS avg_assists,
       ROUND(AVG(damage_dealt), 2) AS avg_damage,
       ROUND(AVG(damage_taken), 2) AS avg_damage_taken,
       ROUND(AVG(vision_score), 2) AS avg_vision,
       ROUND(AVG(gold_earned), 2) AS avg_gold,
       ROUND(AVG(cs_total), 2) AS avg_cs
FROM player_performances
WHERE is_friend = TRUE AND persona IS NOT NULL
GROUP BY pool_id, queue_id, friends_count, persona, COALESCE(role, lane, 'UNKNOWN');

-- 11. Records personales
CREATE VIEW metric_11_records AS
SELECT pool_id, queue_id, friends_count, persona,
       MAX(kills) AS max_kills,
       MAX(deaths) AS max_deaths,
       MAX(assists) AS max_assists,
       MAX(vision_score) AS max_vision_score,
       MAX(cs_total) AS max_cs,
       MAX(damage_dealt) AS max_damage_dealt,
       MAX(gold_earned) AS max_gold,
       MAX(duration_s) AS max_duration_s
FROM player_performances
WHERE is_friend = TRUE AND persona IS NOT NULL
GROUP BY pool_id, queue_id, friends_count, persona;

-- 12. Botlane Synergy
CREATE VIEW metric_12_botlane_synergy AS
WITH adcs AS (
    SELECT match_id, team_id, pool_id, queue_id, friends_count, persona, win, kills, deaths, assists, damage_dealt, vision_score, gold_earned, cs_total
    FROM player_performances
    WHERE is_friend = TRUE AND COALESCE(role, lane) IN ('BOTTOM', 'ADC') AND persona IS NOT NULL
),
sups AS (
    SELECT match_id, team_id, pool_id, queue_id, friends_count, persona, kills, deaths, assists, damage_dealt, vision_score, gold_earned, cs_total
    FROM player_performances
    WHERE is_friend = TRUE AND COALESCE(role, lane) IN ('UTILITY', 'SUPPORT') AND persona IS NOT NULL
)
SELECT a.pool_id, a.queue_id, a.friends_count,
       LEAST(a.persona, s.persona) AS p1,
       GREATEST(a.persona, s.persona) AS p2,
       COUNT(*) AS games,
       SUM(CASE WHEN a.win THEN 1 ELSE 0 END) AS wins,
       ROUND((SUM(CASE WHEN a.win THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) AS winrate,
       ROUND(AVG(
           (a.kills+a.assists)::numeric / GREATEST(a.deaths, 1) + 
           (s.kills+s.assists)::numeric / GREATEST(s.deaths, 1)
       ), 2) AS avg_combined_kda,
       ROUND(AVG(a.damage_dealt + s.damage_dealt), 2) AS avg_combined_damage,
       ROUND(AVG(a.gold_earned + s.gold_earned), 2) AS avg_combined_gold,
       ROUND(AVG(a.vision_score + s.vision_score), 2) AS avg_combined_vision
FROM adcs a
JOIN sups s ON a.match_id = s.match_id AND a.team_id = s.team_id AND a.pool_id = s.pool_id
WHERE a.persona <> s.persona
GROUP BY a.pool_id, a.queue_id, a.friends_count, LEAST(a.persona, s.persona), GREATEST(a.persona, s.persona);

-- 14. Campeones jugados (Comunidad)
CREATE VIEW metric_14_community_champions AS
SELECT pool_id, queue_id, friends_count, champion_name,
       COUNT(*) AS games,
       SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
       SUM(CASE WHEN NOT win THEN 1 ELSE 0 END) AS losses,
       ROUND((SUM(CASE WHEN win THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) AS winrate
FROM player_performances
WHERE is_friend = TRUE 
GROUP BY pool_id, queue_id, friends_count, champion_name;

-- 15. Campeones jugados por el enemigo
CREATE VIEW metric_15_enemy_champions AS
SELECT pool_id, queue_id, friends_count, champion_name,
       COUNT(*) AS games,
       SUM(CASE WHEN win THEN 1 ELSE 0 END) AS wins,
       SUM(CASE WHEN NOT win THEN 1 ELSE 0 END) AS losses,
       ROUND((SUM(CASE WHEN win THEN 1 ELSE 0 END)::numeric / COUNT(*)) * 100, 2) AS winrate
FROM player_performances
WHERE is_friend = FALSE 
GROUP BY pool_id, queue_id, friends_count, champion_name;
