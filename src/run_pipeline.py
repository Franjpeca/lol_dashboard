import subprocess
import sys
from pathlib import Path
from queue import Queue

BASE = Path(__file__).resolve().parents[0]
EXTRACT = BASE / "extract"
LOAD = BASE / "load"
METRICS = BASE / "metrics"

def stream_reader(stream, run_in_terminal, queue):
    for line in iter(stream.readline, ""):
        if run_in_terminal:
            print(line, end="")
        queue.put(line)
    stream.close()

def run_step(name, script, *args, run_in_terminal, queue):
    import threading
    
    header = f"\n======================================\n[PIPELINE] Ejecutando: {name}\n======================================\n"
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

    footer = f"[PIPELINE] Finalizado {name}\n"
    if run_in_terminal:
        print(footer, end="")
    queue.put(footer)
    
    return process.returncode == 0

def run_l0_only(run_in_terminal: bool = True, queue: Queue = None):
    """Ejecuta solo L0: crear índice de usuarios + descargar partidas"""
    if queue is None:
        queue = Queue()
    
    steps = [
        ("L0 - Crear índice de usuarios", EXTRACT / "0_createUserIndex.py", []),
        ("L0 - Descargar partidas desde API", EXTRACT / "0_getAllMatchesFromAPI.py", []),
    ]

    for name, script, extra_args in steps:
        success = run_step(name, script, *extra_args, run_in_terminal=run_in_terminal, queue=queue)
        if not success:
            err_msg = f"[PIPELINE] Error en paso: {name}. Abortando.\n"
            if run_in_terminal:
                print(err_msg)
            queue.put(err_msg)
            return False
    
    return True

def run_l1_to_l3(min_friends: int, pool_id: str = None, run_in_terminal: bool = True, queue: Queue = None):
    """Ejecuta L1 → L2 → L3: filtrar + aplanar + métricas"""
    if queue is None:
        queue = Queue()
    
    # Build arguments for all stages
    l1_args = ["--min", str(min_friends)]
    l2_args = ["--min", str(min_friends)]
    l3_args = ["--min", str(min_friends)]
    
    if pool_id:
        l1_args.extend(["--pool", pool_id])
        l2_args.extend(["--pool", pool_id])
        l3_args.extend(["--pool", pool_id])
        
        if pool_id == "season":
            l3_args.extend(["--start", "2026-01-08"])
    
    steps = [
        ("L1 - Crear colecciones filtradas", LOAD / "1_createFilteredCollections.py", l1_args),
        ("L2 - Construir colecciones L2", LOAD / "2_createL2Collections.py", l2_args),
        ("L3 - Metricas", METRICS / "metricsMain.py", l3_args)
    ]

    for name, script, extra_args in steps:
        success = run_step(name, script, *extra_args, run_in_terminal=run_in_terminal, queue=queue)
        if not success:
            err_msg = f"[PIPELINE] Error en paso: {name}. Abortando.\n"
            if run_in_terminal:
                print(err_msg)
            queue.put(err_msg)
            return False
    
    return True

def main_full(min_friends: int, pool_id: str = None, run_in_terminal: bool = True, queue: Queue = None):
    """Ejecuta el pipeline completo L0 → L3"""
    if queue is None:
        queue = Queue()
    
    # Build arguments for all stages
    l1_args = ["--min", str(min_friends)]
    l2_args = ["--min", str(min_friends)]
    l3_args = ["--min", str(min_friends)]
    
    if pool_id:
        l1_args.extend(["--pool", pool_id])
        l2_args.extend(["--pool", pool_id])
        l3_args.extend(["--pool", pool_id])
        
        if pool_id == "season":
            l3_args.extend(["--start", "2026-01-08"])
    
    steps = [
        ("L0 - Crear índice de usuarios", EXTRACT / "0_createUserIndex.py", []),
        ("L0 - Descargar partidas desde API", EXTRACT / "0_getAllMatchesFromAPI.py", []),
        ("L1 - Crear colecciones filtradas", LOAD / "1_createFilteredCollections.py", l1_args),
        ("L2 - Construir colecciones L2", LOAD / "2_createL2Collections.py", l2_args),
        ("L3 - Metricas", METRICS / "metricsMain.py", l3_args)
    ]

    for name, script, extra_args in steps:
        success = run_step(name, script, *extra_args, run_in_terminal=run_in_terminal, queue=queue)
        if not success:
            err_msg = f"[PIPELINE] Error en paso: {name}. Abortando.\n"
            if run_in_terminal:
                print(err_msg)
            queue.put(err_msg)
            return False
    
    return True

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ejecutar pipeline completo o parcial")
    parser.add_argument("--mode", choices=["l0", "l1-l3", "full"], default="full", help="Modo de ejecución")
    parser.add_argument("--min", type=int, default=5, help="Min friends")
    parser.add_argument("--pool", type=str, default=None, help="Pool ID (optional)")
    parser.add_argument("--run-in-terminal", action="store_true", help="Ejecutar en el terminal principal")
    args = parser.parse_args()

    pool_id = args.pool if hasattr(args, 'pool') else None
    
    if args.mode == "l0":
        run_l0_only(args.run_in_terminal)
    elif args.mode == "l1-l3":
        run_l1_to_l3(args.min, pool_id, args.run_in_terminal)
    else:
        main_full(args.min, pool_id, args.run_in_terminal)
