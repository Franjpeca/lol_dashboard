#!/usr/bin/env python3
import sys
import subprocess
from pathlib import Path

# Rutas
ROOT = Path(__file__).resolve().parents[1]
PIPELINE = ROOT / "src" / "pipeline.py"

def run_command(cmd):
    print(f"\n[ORCHESTRATOR] Ejecutando: {' '.join(cmd)}")
    try:
        # Usamos stdout=None para que se vea el progreso en la terminal directamente
        subprocess.run(cmd, check=True, cwd=str(ROOT))
    except subprocess.CalledProcessError as e:
        print(f"\n[ERROR] El comando falló con código {e.returncode}")
        return False
    return True

def main():
    print("="*50)
    print("LO L DASHBOARD - ORQUESTADOR COMPLETO")
    print("Procesando L1-L2 y Season (min 1 a 5)")
    print("="*50)

    # 1. Pipeline Normal (L1-L2)
    print("\n>>> MODO: L1-L2 (Normal)")
    for min_f in range(1, 6):
        print(f"\n--- Procesando min_friends={min_f} ---")
        cmd = [sys.executable, str(PIPELINE), "--mode", "l1-l2", "--min", str(min_f), "--run-in-terminal"]
        if not run_command(cmd):
            print("Abortando por error en modo l1-l2")
            sys.exit(1)

    # 2. Pipeline Season
    print("\n>>> MODO: SEASON")
    for min_f in range(1, 6):
        print(f"\n--- Procesando min_friends={min_f} (Season) ---")
        cmd = [sys.executable, str(PIPELINE), "--mode", "season", "--min", str(min_f), "--run-in-terminal"]
        if not run_command(cmd):
            print("Abortando por error en modo season")
            sys.exit(1)

    print("\n" + "="*50)
    print("¡PIPELINE COMPLETO FINALIZADO CON ÉXITO!")
    print("="*50)

if __name__ == "__main__":
    main()
