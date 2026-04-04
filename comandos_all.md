Este documento contiene los comandos necesarios para gestionar el ciclo de vida de los datos.

## 🚀 Comando Recomendado: Actualización Total
Si quieres actualizarlo TODO (usuarios, nuevas partidas de normal y season, y todas las tablas del dashboard con sus filtros de 1 a 5 amigos) de una sola vez, usa este script:

```bash
python src/run_all.py
```

---

## 1. Actualizar Cuentas y Personas (Índice)
Este paso lee los archivos JSON en `data/` (`mapa_cuentas.json` y `mapa_cuentas_season.json`) y actualiza quién es quién en la base de datos MongoDB.

```bash
# Actualizar TODOS los índices (Normal y Season) - RECOMENDADO
python src/extract/ingest_users.py

# Actualizar solo uno (opcional)
python src/extract/ingest_users.py --mode normal
python src/extract/ingest_users.py --mode season
```

---

## 2. Descargar Partidas (Ingesta L0)
Descarga las partidas desde la API de Riot para los usuarios configurados en los índices.

```bash
# Descargar partidas de TODOS los jugadores registrados (Normal + Season) - RECOMENDADO
python src/extract/ingest_matches.py

# Descargar partidas de un grupo específico (opcional)
python src/extract/ingest_matches.py --mode normal
python src/extract/ingest_matches.py --mode season
```

---

## 3. Ejecución de Pools (Carga PostgreSQL)
Calcula las métricas y llena las tablas del Dashboard filtrando por el mínimo de amigos presentes.

### 🔵 Pool Normal
Se pueden ejecutar diferentes configuraciones de "mínimo de amigos" para la misma pool.

```bash
# Ejecutar Pool Normal para 5 Amigos (Toda la historia)
python src/pipeline.py --mode full --min 5 --run-in-terminal

# Ejecutar Pool Normal para 4 Amigos (Si ya descargaste partidas antes)
python src/pipeline.py --mode l1-l2 --min 4 --run-in-terminal
```

### 🏆 Pool Season
Procesamiento específico para la temporada actual (fechas fijas).

```bash
# Ejecutar Pool Season (5 amigos por defecto)
python src/pipeline.py --mode season --min 5 --run-in-terminal
```

---

## 4. Limpieza de Datos (Administración)
Si necesitas borrar los datos de una pool para regenerarlos de cero.

```bash
# Borrar todos los datos de la pool Normal
python scripts/delete_pool_data.py ca879f16

# Borrar todos los datos de la pool Season
python scripts/delete_pool_data.py season
```

> [!NOTE]
> El ID de la pool normal suele ser `ca879f16`. Puedes verificarlo en el Dashboard o en la base de datos.
