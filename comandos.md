# 🎮 Comandos y Orquestadores - LoL Dashboard

Esta guía detalla los comandos necesarios para operar el pipeline de datos, desde la descarga hasta la visualización en el dashboard.

---

## 🛠️ 0. Preparación (Entorno)
Antes de ejecutar cualquier comando, asegúrate de estar en la carpeta raíz y activar el entorno virtual.

```bash
# Entrar en la carpeta
cd /home/dev/lol_dashboard

# Activar entorno virtual
source .venv/bin/activate
```

---

## 📥 1. Descarga de Partidas (L0)
Este paso se conecta a la API de Riot Games para bajar las partidas nuevas de todos los jugadores registrados.

**Comando:**
```bash
python src/pipeline.py --mode l0 --run-in-terminal
```
*   **¿Qué hace?**: Ejecuta `ingest_users.py` (actualiza quiénes somos) e `ingest_matches.py` (baja partidas nuevas).
*   **Nota**: Solo baja partidas que aún no tienes en la base de datos.

---

## ⚙️ 2. Pipeline General (ETL)
Procesa las partidas descargadas (datos crudos) y las convierte en estadísticas listas para el Dashboard (PostgreSQL).

Existen dos orquestadores principales:

### A. Orquestador Automático (Recomendado)
Procesa **todos** los filtros de amigos (1 a 5) tanto para el pool Normal como para Season. Es el que debes lanzar para un refresco total.

**Comando:**
```bash
python scripts/run_pipeline_all.py
```

### B. Orquestador Manual (Por Pool)
Si solo quieres actualizar una sección específica rápidamente:

*   **Actualizar Pool Normal (General):**
    ```bash
    python src/pipeline.py --mode l1-l2 --min 1 --run-in-terminal
    ```
*   **Actualizar Pool Season (2026):**
    ```bash
    python src/pipeline.py --mode season --min 2 --run-in-terminal
    ```

---

## 🚀 3. Lanzar el Servidor (Dashboard)
Arranca la interfaz web en el puerto configurado (`8080`).

**Comando:**
```bash
streamlit run dashboard/app.py --server.port 8080
```

---

## 📒 Resumen de Scripts Orquestadores

| Script | Propósito |
| :--- | :--- |
| `src/pipeline.py` | **Orquestador Base**. Permite lanzar fases específicas (`l0`, `l1-l2`, `season`) mediante flags. |
| `scripts/run_pipeline_all.py` | **Orquestador de Lazo**. Llama a `pipeline.py` iterativamente para cubrir todos los casos de "Min Amigos" (1-5) automáticamente. |
| `src/extract/ingest_matches.py` | Script de bajo nivel para bajar partidas (usado por el pipeline). |
| `src/load/populate_pg.py` | Script de bajo nivel que mueve datos de Mongo a Postgres (usado por el pipeline). |
