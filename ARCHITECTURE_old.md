# Arquitectura del Proyecto `lol_data`

> Documento de referencia. Refleja el estado actual del sistema tras el refactoring de Marzo 2026.

---

## Visión General

Pipeline de datos para analizar partidas de League of Legends de un grupo de amigos. Extrae datos de la Riot API, los carga en MongoDB en capas (L0 → L1 → L2), calcula métricas y las guarda como JSON.

```
Riot API
    │
    ▼
[EXTRACT]  0_createUserIndex.py        → MongoDB: L0_users_index
[EXTRACT]  0_getAllMatchesFromAPI.py   → MongoDB: L0_all_raw_matches
                                        MongoDB: riot_accounts
    │
    │   (alternativa: cargar desde JSONs cacheados en disco)
[LOAD L0]  0_getRawMatchesFromFile.py → MongoDB: L0_all_raw_matches
    │
    ▼
[LOAD L1]  1_createFilteredCollections.py → MongoDB: L1_q{queue}_min{N}_{pool_id}
    │
    ▼
[LOAD L2]  2_createL2Collections.py  → MongoDB: L2_players_flat_{suffix}
                                      → MongoDB: L2_enemies_flat_{suffix}
                                      → MongoDB: L2_matches_summary_{suffix}
    │
    ▼
[METRICS]  metricsMain.py + 13 scripts → JSON en data/runtime/pool_{id}/q{n}/min{n}/
```

---

## Fuente de Verdad: `mapa_cuentas.json`

Mapea nombres reales (personas) → lista de cuentas de Riot que han usado:

```json
{
  "Fran": ["Romance Ðawn#AGZ", "Frieren#Hana", ...],
  "Olaf": ["The Gatebreaker#1707", ...],
  ...
}
```

**Regla:** Nunca editar directamente `L0_users_index`. Editar `mapa_cuentas.json` y re-ejecutar el script de extracción.

---

## Colecciones MongoDB (`lol_data`)

| Colección | Generada por | Contenido |
|---|---|---|
| `L0_users_index` | `0_createUserIndex.py` | Un doc por persona. Campos: `persona`, `riotIds[]`, `puuids[]`, `accounts[]` |
| `L0_all_raw_matches` | `0_getAllMatchesFromAPI.py` / `0_getRawMatchesFromFile.py` | JSON raw de cada partida tal como viene de la API de Riot. `_id = matchId` |
| `riot_accounts` | Ambos scripts de extracción | Cuentas conocidas con `puuid`, `riotId`, `region`, historial de nombres |
| `L1_q{queue}_min{N}_{pool_id}` | `1_createFilteredCollections.py` | Partidas filtradas: solo las del queue indicado con ≥ N amigos del pool presentes |
| `L2_players_flat_{suffix}` | `2_createL2Collections.py` | Un doc por participante amigo por partida (desnormalizado) |
| `L2_enemies_flat_{suffix}` | `2_createL2Collections.py` | Idem para enemigos |
| `L2_matches_summary_{suffix}` | `2_createL2Collections.py` | Resumen de la partida: duración, equipos, personas presentes |

### ¿Qué es el `pool_id`?

Es un hash SHA-1 de 8 caracteres calculado a partir del **conjunto de personas** en `L0_users_index`. Ejemplo: `pool_ac89fa8d`.

```python
# Fuente de verdad: utils/pool_manager.py → build_pool_version(personas)
base = ",".join(sorted(personas))   # ["Ainara", "Ayna", "Carla", ...]
hash = sha1(base).hexdigest()[:8]   # "ac89fa8d"
pool_id = f"pool_{hash}"
```

**Importante:** El hash se basa en **personas** (no en PUUIDs), para que sea estable cuando un jugador cambia de nombre de cuenta en Riot. Si se añade/elimina una persona del `mapa_cuentas.json`, el hash cambia y se generan nuevas colecciones L1/L2.

---

## Scripts de Extracción (`src/extract/`)

### `0_createUserIndex.py`
- Lee `mapa_cuentas.json`
- Consulta la Riot API para obtener el PUUID actual de cada cuenta
- **Elimina y recrea** `L0_users_index` desde cero
- Ejecutar siempre antes de descargar partidas si `mapa_cuentas.json` ha cambiado

### `0_getAllMatchesFromAPI.py`
- Lee personas y PUUIDs de `L0_users_index`
- Descarga todas las partidas del `QUEUE_FLEX` (440 = Flex) hasta `COUNT_PER_PLAYER` por jugador
- Inserta en `L0_all_raw_matches` solo si no existe (idempotente)
- Actualiza `riot_accounts` con historial de nombres
- Respeta rate limits con retry + backoff exponencial

### `0_getRawMatchesFromFile.py`
- Alternativa sin API: carga partidas desde JSONs en `LOL_CACHE_DIR`
- Útil para cargar datos históricos cacheados en disco
- También sincroniza `riot_accounts` desde `LOL_USERS_DIR`

---

## Scripts de Carga (`src/load/`)

### `1_createFilteredCollections.py`
```bash
python src/load/1_createFilteredCollections.py --queue 440 --min 5 [--pool HASH]
```
- Lee `L0_users_index` para obtener personas y PUUIDs del pool
- Filtra `L0_all_raw_matches` por queue y cantidad mínima de amigos presentes
- Si `--pool season`, aplica filtro adicional de fecha (`gameStartTimestamp >= 2026-01-08`)
- **Drop + recreate** de la colección de destino en cada ejecución

### `2_createL2Collections.py`
```bash
python src/load/2_createL2Collections.py --min 5 [--pool HASH]
```
- Desnormaliza L1 → 3 colecciones L2 (players, enemies, summary)
- Si no se pasa `--pool`, auto-calcula desde `L0_users_index` usando personas
- **Drop + recreate** de las 3 colecciones de destino

---

## Scripts de Métricas (`src/metrics/`)

Orquestador: `metricsMain.py`
```bash
python src/metrics/metricsMain.py --queue 440 --min 5 [--pool HASH] [--start YYYY-MM-DD] [--end YYYY-MM-DD]
```

Ejecuta secuencialmente los 13 scripts de métricas. Cada uno lee de L1/L2 y guarda JSON en:
```
data/runtime/pool_{id}/q{queue}/min{N}/metrics_NN_nombre.json
```

| Script | Métrica |
|---|---|
| `metrics_01` | Win rate por jugador |
| `metrics_02` | Win rate por campeón |
| `metrics_03` | Frecuencia de partidas |
| `metrics_04` | Rachas de victorias/derrotas |
| `metrics_05` | Stats generales por jugador |
| `metrics_06` | Ego index |
| `metrics_07` | Troll index |
| `metrics_08` | Primeras métricas (first blood, etc.) |
| `metrics_09` | Número de habilidades usadas |
| `metrics_10` | Stats por rol |
| `metrics_11` | Records y máximos |
| `metrics_12` | Sinergia de botlane |
| `metrics_13` | Stats por campeón por jugador |

---

## Orquestador del Pipeline (`src/run_pipeline.py`)

```bash
# Pipeline completo (L0 → L3)
python src/run_pipeline.py --mode full --min 5 [--pool HASH]

# Solo extracción (L0)
python src/run_pipeline.py --mode l0

# Solo filtrado + métricas (L1 → L3)
python src/run_pipeline.py --mode l1-l3 --min 5 [--pool HASH]
```

Para la temporada actual usar `--pool season` (aplica filtro de fecha en L1 y en métricas).

---

## Variables de Entorno (`.env`)

| Variable | Uso |
|---|---|
| `MONGO_URI` | Conexión a MongoDB |
| `MONGO_DB` | Nombre de la base de datos (`lol_data`) |
| `MONGO_COLLECTION_RAW_MATCHES` | Colección raw (`L0_all_raw_matches`) |
| `LOL_CACHE_DIR` | Ruta a los JSONs de partidas cacheadas en disco |
| `LOL_USERS_DIR` | Ruta a los JSONs de usuarios conocidos |
| `LOL_PLAYERS_FILE` | Ruta al `players.txt` legado (opcional) |
| `REGIONAL_ROUTING` | Región de la API de Riot (`europe`) |
| `QUEUE_FLEX` | ID de cola (`440` = Flex) |
| `COUNT_PER_PLAYER` | Máximo de partidas a descargar por jugador |
| `MIN_FRIENDS_IN_MATCH` | Mínimo de amigos para incluir una partida |
| `RIOT_API_KEY` | API Key de Riot (renovar si expira) |

---

## Infraestructura Docker

```bash
docker compose up -d
```

| Servicio | Puerto | Descripción |
|---|---|---|
| `mongo` | 27017 | MongoDB — datos crudos y procesados |
| `mongo-express` | 8081 | Panel web para inspeccionar MongoDB |
| `postgres` | 5432 | PostgreSQL — reservado para métricas futuras |
| `adminer` | 8082 | Panel web SQL para PostgreSQL |

Datos persistentes en `./mongo/data/` y `./postgres/data/` (volúmenes locales).

---

## Flujo de Trabajo Típico

```
1. Editar mapa_cuentas.json si hay nueva persona/cuenta
2. python src/run_pipeline.py --mode l0          # Actualizar índice + descargar partidas
3. python src/run_pipeline.py --mode l1-l3 --min 5 --pool season   # Recalcular todo para esta temporada
```

O todo de un tirón:
```
python src/run_pipeline.py --mode full --min 5 --pool season
```
