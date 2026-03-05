# Comandos Diarios del Pipeline LoL

Aquí tienes la lista rápida de comandos para mantener el dashboard actualizado. Todos deben ejecutarse dentro de la carpeta del proyecto en el servidor, con el entorno virtual activado:

```bash
cd /home/dev/lol_dashboard
source .venv/bin/activate
```

---

## 1. Descargar partidas nuevas
**Ejecutar:** Cuando hayas jugado partidas nuevas que quieras incorporar al dashboard.
*(Este comando buscará en la API de Riot y solo descargará las partidas recientes que no tengas en tu base de datos).*

```bash
python src/pipeline.py --mode l0 --source api --run-in-terminal
```

---

## 2. Actualizar datos para el "Min Amigos" (Pool Normal)
**Ejecutar:** Después de descargar partidas nuevas (paso 1).
*(Esto cogerá TODAS tus partidas desde los inicios y recalculará estadísticas para el pool general, filtrando automáticamente el número real de amigos presentes en cada partida para que el desplegable del dashboard funcione).*

```bash
python src/pipeline.py --mode l1-l2 --min 1 --run-in-terminal
```

---

## 3. Actualizar datos para "Season" (Partidas en 2026)
**Ejecutar:** Después de descargar partidas nuevas (paso 1).
*(Esto cogerá las partidas desde el 8 de enero de 2026 y actualizará el pool exclusivo de Season. El flag `--min 2` asume que para season solo cuentan las partidas con mínimo 2 amigos).*

```bash
python src/pipeline.py --mode season --min 2 --run-in-terminal
```

---

## 4. [USO RARO] Ingestar el histórico desde Local (Windows)
**Ejecutar:** SOLO si el MongoDB del servidor se borra por accidente o si has añadido miles de archivos JSON antiguos a mano desde Windows a la carpeta `/data/match_cache`.

```bash
python src/pipeline.py --mode l0 --source file --run-in-terminal
```

---

## 5. Levantar el dashboard
**Ejecutar:** Para arrancar la página web si se ha caído.

```bash
streamlit run dashboard/app.py
```
