-- clean_and_reload_prep.sql
-- Limpia datos incompletos de player_performances y matches para recargar limpio.
-- Ejecutar ANTES de relanzar el pipeline con --min 1.

BEGIN;

-- 1. Actualizar friends_count = NULL con el valor real (desde friends_present)
UPDATE player_performances pp
SET friends_count = sub.real_count
FROM (
    SELECT match_id, pool_id,
           cardinality(friends_present) AS real_count
    FROM matches
) sub
WHERE pp.match_id = sub.match_id
  AND pp.pool_id  = sub.pool_id
  AND pp.friends_count IS NULL;

-- 2. Verificar cuántas filas hay por pool/friends_count ANTES de limpiar
-- (Descomenta para ver el estado actual)
-- SELECT pool_id, friends_count, COUNT(*) AS rows, COUNT(DISTINCT match_id) AS matches
-- FROM player_performances
-- GROUP BY pool_id, friends_count ORDER BY pool_id, friends_count;

COMMIT;

SELECT 'Limpieza aplicada. Ahora recarga con: python src/pipeline.py --mode l1-l2 --min 1 --run-in-terminal' AS siguiente_paso;
