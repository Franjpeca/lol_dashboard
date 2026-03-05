-- migrate_pools_pk.sql
-- Migración: cambia la PK de pools de (pool_id) a (pool_id, min_friends)
-- Ejecutar UNA sola vez en la BD existente.
--
-- Uso:
--   psql -U lol_user -d lol_analytics -f scripts/migrate_pools_pk.sql
-- O desde Docker:
--   docker exec -it <container> psql -U lol_user -d lol_analytics -f /scripts/migrate_pools_pk.sql

BEGIN;

-- 1. Quitar la FK de matches → pools (para poder alterar la PK)
ALTER TABLE matches DROP CONSTRAINT IF EXISTS matches_pool_id_fkey;

-- 2. Eliminar la PK actual (solo pool_id)
ALTER TABLE pools DROP CONSTRAINT IF EXISTS pools_pkey;

-- 3. Añadir la nueva PK compuesta
ALTER TABLE pools ADD PRIMARY KEY (pool_id, min_friends);

-- 4. No restauramos la FK de matches → pools porque matches tiene (pool_id, queue_id)
--    pero no min_friends, por lo que no puede referenciar la nueva PK compuesta.
--    matches.pool_id sigue siendo una referencia lógica, sin constraint formal.

COMMIT;
