"""
pipeline.py — Único punto de entrada al pipeline de datos LoL.

Modos disponibles:
  l0      → Ingesta de usuarios + partidas desde API
  l1-l3   → Filtrado, aplanado y cálculo de métricas
  full    → Pipeline completo (l0 + l1-l3)
  season  → Pipeline de temporada con fechas fijas (2026-01-08 a hoy)

Ejemplos:
  python src/pipeline.py --mode full --run-in-terminal
  python src/pipeline.py --mode l1-l3 --min 5
  python src/pipeline.py --mode season --run-in-terminal
"""

import sys
import subprocess
import threading
import argparse
from pathlib import Path
from queue import Queue
from datetime import date
from utils.status import save_last_update

sys.stdout.reconfigure(encoding='utf-8')

# Rutas
FILE_SELF = Path(__file__).resolve()
SRC_DIR = FILE_SELF.parent
BASE_DIR = SRC_DIR.parent

EXTRACT = SRC_DIR / "extract"
LOAD = SRC_DIR / "load"
METRICS = SRC_DIR / "metrics"

# Cola para integración con UIs externas (e.g. Dash)
PIPELINE_QUEUE: Queue = Queue()

# Constantes Season
SEASON_POOL_ID = "season"
SEASON_START_DATE = "2026-01-08"
SEASON_USERS_COLLECTION = "L0_users_index_season"


# =============================
# RUNNER INTERNO
# =============================

def _stream_reader(stream, run_in_terminal: bool, queue: Queue):
    for line in iter(stream.readline, ""):
        if run_in_terminal:
            print(line, end="", flush=True)
        queue.put(line)
    stream.close()


def run_step(name: str, script: Path, *args, run_in_terminal: bool, queue: Queue) -> bool:
    header = f"\n{'='*38}\n[PIPELINE] {name}\n{'='*38}\n"
    if run_in_terminal:
        print(header, end="")
    queue.put(header)

    cmd = [sys.executable, str(script)] + [str(a) for a in args]
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(BASE_DIR),
    )

    t_out = threading.Thread(target=_stream_reader, args=(process.stdout, run_in_terminal, queue))
    t_err = threading.Thread(target=_stream_reader, args=(process.stderr, run_in_terminal, queue))
    t_out.start(); t_err.start()
    t_out.join(); t_err.join()
    process.wait()

    status_ok = process.returncode == 0
    footer = f"[PIPELINE] {'OK' if status_ok else 'ERROR'} — {name}\n"
    if run_in_terminal:
        print(footer, end="")
    queue.put(footer)
    return status_ok


def _abort(name: str, run_in_terminal: bool, queue: Queue):
    msg = f"[PIPELINE] Error en: {name}. Abortando.\n"
    if run_in_terminal:
        print(msg)
    queue.put(msg)


# =============================
# MODOS
# =============================

def run_l0(run_in_terminal: bool, queue: Queue, mode: str = "normal") -> bool:
    """Ingesta de usuarios + partidas."""
    steps = [
        ("L0 — Índice de usuarios",   EXTRACT / "ingest_users.py",   ["--mode", mode]),
        ("L0 — Partidas desde API",   EXTRACT / "ingest_matches.py", ["--mode", mode]),
    ]
    for name, script, args in steps:
        if not run_step(name, script, *args, run_in_terminal=run_in_terminal, queue=queue):
            _abort(name, run_in_terminal, queue)
            return False
    return True


def run_l1_to_l2(min_friends: int, pool_id: str | None,
                  run_in_terminal: bool, queue: Queue) -> bool:
    """Filtrado L1 (Mongo) → ETL L2 a PostgreSQL."""
    base_args = ["--min", str(min_friends)]
    if pool_id:
        base_args += ["--pool", pool_id]

    steps = [
        ("L1 — Colecciones filtradas (Mongo)",  LOAD / "build_L1_filtered.py",  base_args),
        ("L2 — ETL: Mongo L1 → PostgreSQL",     LOAD / "populate_pg.py",         base_args),
    ]
    for name, script, args in steps:
        if not run_step(name, script, *args, run_in_terminal=run_in_terminal, queue=queue):
            _abort(name, run_in_terminal, queue)
            return False
    return True



def run_full(min_friends: int, pool_id: str | None,
             run_in_terminal: bool, queue: Queue, skip_l0: bool = False) -> bool:
    """Pipeline completo L0 (Mongo) → L2 (PostgreSQL)."""
    if not skip_l0:
        if not run_l0(run_in_terminal, queue, mode="normal"):
            return False
    return run_l1_to_l2(min_friends, pool_id, run_in_terminal, queue)


def run_season(min_friends: int, run_in_terminal: bool, queue: Queue, skip_l0: bool = False) -> bool:
    """Pipeline de temporada con fechas fijas."""
    end_date = date.today().isoformat()
    common = ["--min", str(min_friends), "--pool", SEASON_POOL_ID,
              "--users-collection", SEASON_USERS_COLLECTION]

    # Ingesta L0 Season (Usuarios + Partidas)
    if not skip_l0:
        if not run_l0(run_in_terminal, queue, mode="season"):
            return False

    # Luego L1 → ETL PG con parámetros season
    steps = [
        ("L1 Season — Filtrado (Mongo)",        LOAD / "build_L1_filtered.py",  common),
        ("L2 Season — ETL: Mongo L1 → PG",      LOAD / "populate_pg.py",         common + ["--users-collection", SEASON_USERS_COLLECTION]),
    ]
    for name, script, args in steps:
        if not run_step(name, script, *args, run_in_terminal=run_in_terminal, queue=queue):
            _abort(name, run_in_terminal, queue)
            return False

    queue.put(f"[PIPELINE] ✅ Season completado ({SEASON_START_DATE} → {end_date})\n")
    return True


# =============================
# ENTRY POINT
# =============================

if __name__ == "__main__":
    # Import config solo al ejecutar, no al importar como módulo
    sys.path.insert(0, str(SRC_DIR))
    from utils.config import MIN_FRIENDS_IN_MATCH

    parser = argparse.ArgumentParser(description="LoL Data Pipeline")
    parser.add_argument("--mode", choices=["l0", "l1-l2", "full", "season"],
                        default="full", help="Modo de ejecución")
    parser.add_argument("--min", type=int, default=MIN_FRIENDS_IN_MATCH,
                        help="Mínimo de amigos en partida")
    parser.add_argument("--pool", type=str, default=None,
                        help="Pool ID (hash 8 chars, o 'season')")
    parser.add_argument("--run-in-terminal", action="store_true",
                        help="Mostrar output en tiempo real")
    parser.add_argument("--skip-l0", action="store_true",
                        help="Saltar ingesta L0 (usuarios y partidas)")
    args = parser.parse_args()

    q = PIPELINE_QUEUE
    rt = args.run_in_terminal

    if args.mode == "l0":
        run_l0(rt, q)
    elif args.mode == "l1-l2":
        run_l1_to_l2(args.min, args.pool, rt, q)
    elif args.mode == "season":
        run_season(args.min, rt, q, skip_l0=args.skip_l0)
    else:
        run_full(args.min, args.pool, rt, q, skip_l0=args.skip_l0)

    # Actualizar marca de tiempo
    save_last_update()
