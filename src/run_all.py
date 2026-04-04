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

def main():
    print("=" * 60)
    print("🔥 INICIANDO ACTUALIZACIÓN TOTAL DEL DASHBOARD 🔥")
    print("=" * 60)

    # 1. Actualizar Usuarios y Descargar Partidas (Unificado)
    # Ahora ingest_users.py e ingest_matches.py procesan Normal y Season por defecto
    print("\n--- PASO 1: Actualizando índices de usuarios (Unificado) ---")
    run_command(["src/extract/ingest_users.py"])

    print("\n--- PASO 2: Descargando todas las partidas nuevas (Riot API) ---")
    run_command(["src/extract/ingest_matches.py"])

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
