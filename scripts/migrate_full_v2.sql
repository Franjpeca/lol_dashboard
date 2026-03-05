-- migrate_full_v2.sql
-- Migración completa v2: soporte para múltiples pools y friends_count
BEGIN;

-- 1. Tablas temporales para salvaguardar datos si fuera necesario (opcional)
-- Aquí simplemente vamos a recrear o alterar.

-- 2. Modificar pools (ya hecho antes, pero por si acaso)
ALTER TABLE IF EXISTS pools DROP CONSTRAINT IF EXISTS pools_pkey CASCADE;
ALTER TABLE pools ADD PRIMARY KEY (pool_id, min_friends);

-- 3. Modificar matches
ALTER TABLE IF EXISTS matches DROP CONSTRAINT IF EXISTS matches_pkey CASCADE;
ALTER TABLE matches ADD PRIMARY KEY (match_id, pool_id);

-- 4. Modificar player_performances
ALTER TABLE player_performances ADD COLUMN IF NOT EXISTS friends_count INTEGER;
-- Actualizar friends_count desde la tabla matches
UPDATE player_performances pp
SET friends_count = cardinality(m.friends_present)
FROM matches m
WHERE pp.match_id = m.match_id AND pp.pool_id = m.pool_id;

-- 5. Añadir restricción única a player_performances
-- Primero limpiamos posibles duplicados que hayan quedado por fallos anteriores
DELETE FROM player_performances pp1
USING player_performances pp2
WHERE pp1.id > pp2.id 
  AND pp1.match_id = pp2.match_id 
  AND pp1.pool_id = pp2.pool_id 
  AND pp1.puuid = pp2.puuid;

ALTER TABLE player_performances ADD CONSTRAINT pp_unique_match_pool_puuid UNIQUE (match_id, pool_id, puuid);

COMMIT;
