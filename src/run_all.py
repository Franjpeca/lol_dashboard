import json
import datetime
import subprocess
import sys
import os
from pathlib import Path

# Configuración de rutas
SRC_DIR = Path(__file__).resolve().parent
ROOT_DIR = SRC_DIR.parent

def run_command(cmd_list):
    """Ejecuta un comando de sistema y muestra la salida en tiempo real."""
    print(f"\n[RUN_ALL] 🚀 Ejecutando: {' '.join(cmd_list)}")
    try:
        # Usamos stdout=None y stderr=None para que la salida se vea directamente en la terminal
        # Esto permite ver las barras de progreso y colores de los scripts originales
        subprocess.run([sys.executable] + cmd_list, check=True, cwd=ROOT_DIR)
    except subprocess.CalledProcessError as e:
        print(f"\n[RUN_ALL] ❌ Error en el comando: {' '.join(cmd_list)}")
        return False
    return True

from utils.status import save_last_update

import argparse

def main():
    parser = argparse.ArgumentParser(description="Actualización total del Dashboard (Ingesta + ETL)")
    parser.add_argument("--limit", type=int, default=15, help="Límite de partidas por jugador (defecto 15)")
    parser.add_argument("--all", action="store_true", help="Descargar TODO el historial")
    args_cli = parser.parse_args()

    print("=" * 60)
    print("🔥 INICIANDO ACTUALIZACIÓN TOTAL DEL DASHBOARD 🔥")
    print(f"   Modo: {'COMPLETO (--all)' if args_cli.all else f'RÁPIDO (Límite {args_cli.limit} partidas)'}")
    print("=" * 60)

    now_diag = datetime.datetime.now(datetime.timezone.utc)
    print(f"[DIAG] Hora actual (UTC): {now_diag.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[DIAG] Hora actual (Local): {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Actualizar Usuarios
    print("\n--- PASO 1: Actualizando índices de usuarios (Normal + Season) ---")
    run_command(["src/extract/ingest_users.py"])
    run_command(["src/extract/ingest_users.py", "--mode", "season"])

    # 2. Descargar Partidas con los argumentos recibidos
    print("\n--- PASO 2: Descargando partidas nuevas (Riot API) ---")
    ingest_cmd = ["src/extract/ingest_matches.py"]
    if args_cli.all:
        ingest_cmd.append("--all")
    else:
        ingest_cmd.extend(["--limit", str(args_cli.limit)])
    
    run_command(ingest_cmd)

    # 3. Procesar Pools Normales (Min 1 a 5)
    # Usamos mode l1-l2 para generar las tablas filtradas sin volver a descargar
    print("\n--- PASO 3: Generando tablas para Pool Normal (Filtros 1-5 amigos) ---")
    for m in range(1, 6):
        print(f"\n>> Procesando Pool Normal (min_friends={m})...")
        run_command(["src/pipeline.py", "--mode", "l1-l2", "--min", str(m), "--run-in-terminal"])

    # 4. Procesar Pool Season
    # Usamos --skip-l0 porque ya descargamos las partidas en el paso 2
    print("\n--- PASO 4: Generando tablas para Pool Season ---")
    run_command(["src/pipeline.py", "--mode", "season", "--min", "5", "--skip-l0", "--run-in-terminal"])

    print("\n" + "=" * 60)
    print("✅ PROCESO COMPLETADO EXITOSAMENTE")
    print("=" * 60)
    
    save_last_update()

if __name__ == "__main__":
    main()
