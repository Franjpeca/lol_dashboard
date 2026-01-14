"""
run_pipeline_season.py
Pipeline específico para el modo SEASON que:
1. Usa mapa_cuentas_season.json
2. Escribe a L0_users_index_season
3. Usa pool fija "season"
4. Genera métricas con fechas fijas (8 enero 2026 a hoy)
"""

import subprocess
import sys
from pathlib import Path
from queue import Queue
from datetime import date

BASE = Path(__file__).resolve().parents[0]
EXTRACT = BASE / "extract"
LOAD = BASE / "load"
METRICS = BASE / "metrics"

# Constantes de Season
SEASON_POOL_ID = "season"
SEASON_START_DATE = "2026-01-08"
SEASON_USERS_COLLECTION = "L0_users_index_season"


def stream_reader(stream, run_in_terminal, queue):
    for line in iter(stream.readline, ""):
        if run_in_terminal:
            print(line, end="")
        queue.put(line)
    stream.close()


def run_step(name, script, *args, run_in_terminal, queue):
    import threading
    
    header = f"\n======================================\n[SEASON] Ejecutando: {name}\n======================================\n"
    if run_in_terminal:
        print(header, end="")
    queue.put(header)

    cmd = [sys.executable, str(script)] + list(args)

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace"
    )

    t_out = threading.Thread(
        target=stream_reader,
        args=(process.stdout, run_in_terminal, queue)
    )
    t_err = threading.Thread(
        target=stream_reader,
        args=(process.stderr, run_in_terminal, queue)
    )

    t_out.start()
    t_err.start()

    t_out.join()
    t_err.join()

    process.wait()

    footer = f"[SEASON] Finalizado {name}\n"
    if run_in_terminal:
        print(footer, end="")
    queue.put(footer)
    
    return process.returncode == 0


def run_season_pipeline(min_friends: int = 5, run_in_terminal: bool = True, queue: Queue = None):
    """Ejecuta el pipeline completo para el modo SEASON"""
    if queue is None:
        queue = Queue()
    
    # Fecha fin = hoy
    end_date = date.today().isoformat()
    
    # Argumentos comunes para L1, L2, L3
    common_args = [
        "--min", str(min_friends),
        "--pool", SEASON_POOL_ID,
        "--users-collection", SEASON_USERS_COLLECTION
    ]
    
    # Argumentos de fechas para L3 (métricas)
    l3_args = common_args + ["--start", SEASON_START_DATE, "--end", end_date]
    
    steps = [
        ("L0 Season - Crear índice de usuarios", EXTRACT / "0_createUserIndex_season.py", []),
        ("L1 Season - Crear colecciones filtradas", LOAD / "1_createFilteredCollections.py", common_args),
        ("L2 Season - Construir colecciones L2", LOAD / "2_createL2Collections.py", common_args),
        ("L3 Season - Métricas", METRICS / "metricsMain.py", l3_args)
    ]

    for name, script, extra_args in steps:
        success = run_step(name, script, *extra_args, run_in_terminal=run_in_terminal, queue=queue)
        if not success:
            err_msg = f"[SEASON] Error en paso: {name}. Abortando.\n"
            if run_in_terminal:
                print(err_msg)
            queue.put(err_msg)
            return False
    
    queue.put(f"[SEASON] ✅ Pipeline completado para fechas {SEASON_START_DATE} a {end_date}\n")
    return True


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ejecutar pipeline SEASON")
    parser.add_argument("--min", type=int, default=5, help="Min friends")
    parser.add_argument("--run-in-terminal", action="store_true", default=True, help="Ejecutar en terminal")
    args = parser.parse_args()

    run_season_pipeline(args.min, args.run_in_terminal)
