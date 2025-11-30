import subprocess
import sys
import argparse
from pathlib import Path
import threading
from queue import Queue

BASE = Path(__file__).resolve().parents[0]
EXTRACT = BASE / "extract"
LOAD = BASE / "load"
METRICS = BASE / "metrics"

PIPELINE_QUEUE = Queue()  # Cola para enviar salida a Dash

def stream_reader(stream, run_in_terminal):
    for line in iter(stream.readline, ""):
        if run_in_terminal:
            print(line, end="")
        PIPELINE_QUEUE.put(line)
    stream.close()

def run_step(name, script, *args, run_in_terminal):
    header = f"\n======================================\n[PIPELINE] Ejecutando: {name}\n======================================\n"
    if run_in_terminal:
        print(header, end="")
    PIPELINE_QUEUE.put(header)

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
        args=(process.stdout, run_in_terminal)
    )
    t_err = threading.Thread(
        target=stream_reader,
        args=(process.stderr, run_in_terminal)
    )

    t_out.start()
    t_err.start()

    t_out.join()
    t_err.join()

    process.wait()

    footer = f"[PIPELINE] Finalizado {name}\n"
    if run_in_terminal:
        print(footer, end="")
    PIPELINE_QUEUE.put(footer)

def main(min_friends: int, run_in_terminal: bool = True):

    run_step(
        "L0 - Descargar partidas desde API",
        EXTRACT / "0_getAllMatchesFromAPI.py",
        run_in_terminal=run_in_terminal
    )

    run_step(
        "L1 - Crear colecciones filtradas",
        LOAD / "1_createFilteredCollections.py",
        "--min", str(min_friends),
        run_in_terminal=run_in_terminal
    )

    run_step(
        "L2 - Construir colecciones L2",
        LOAD / "2_createL2Collections.py",
        "--min", str(min_friends),
        run_in_terminal=run_in_terminal
    )

    run_step(
        "L3 - Metricas",
        METRICS / "metricsMain.py",
        "--min", str(min_friends),
        run_in_terminal=run_in_terminal
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ejecutar pipeline completo L0-L3")
    parser.add_argument("--min", type=int, default=5, help="Min friends")
    parser.add_argument("--run-in-terminal", action="store_true", help="Ejecutar en el terminal principal")
    args = parser.parse_args()

    main(args.min, args.run_in_terminal)
