import json
import datetime
import subprocess
import sys
import os
import psycopg2
from pathlib import Path

# Asegurar que src/ esté en sys.path
FILE_SELF = Path(__file__).resolve()
ROOT = FILE_SELF.parents[1]
SRC_DIR = ROOT / "src"
DATA_DIR = ROOT / "data"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

def run_command(cmd_list):
    """Ejecuta un comando de sistema y muestra la salida en tiempo real."""
    print(f"\n[RUN_ALL] 🚀 Ejecutando: {' '.join(cmd_list)}")
    try:
        # Usamos stdout=None y stderr=None para que la salida se vea directamente en la terminal
        # Esto permite ver las barras de progreso y colores de los scripts originales
        subprocess.run([sys.executable] + cmd_list, check=True, cwd=ROOT)
    except subprocess.CalledProcessError as e:
        print(f"\n[RUN_ALL] ❌ Error en el comando: {' '.join(cmd_list)}")
        return False
    return True

from utils.status import save_last_update
from utils.config import POSTGRES_URI

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
    print(f"[DIAG] Hora actual (UTC): {now_diag.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
    print(f"[DIAG] Hora actual (Local): {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    # 0. Detección y Bootstrap de Pools
    print("\n[STEP 0/4] --- PASO 0: Sincronizando Pools con archivos JSON ---", flush=True)
    
    # Detectar pools por archivos
    data_dir = ROOT / "data"
    detected_pools = []
    for f in data_dir.glob("mapa_cuentas_*.json"):
        pid = f.stem.replace("mapa_cuentas_", "")
        if pid == "season": continue
        detected_pools.append(pid)
    
    # Asegurar 'villaquesitos' (caso base si no tiene sufijo)
    if "villaquesitos" not in detected_pools:
        detected_pools.append("villaquesitos")
    
    _PG_DSN = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")
    try:
        conn = psycopg2.connect(_PG_DSN)
        cur = conn.cursor()
        
        # [LIMPIEZA] Borrar IDs antiguos con espacios que ensucian el Dashboard
        print("   > Limpiando IDs antiguos con espacios...", end=" ", flush=True)
        cur.execute("DELETE FROM pools WHERE pool_id LIKE '%% %%'")
        print("OK")

        for pid in detected_pools:
            print(f"   > Asegurando ID técnico en DB: {pid} (niveles 1-5)...", end=" ", flush=True)
            for m in range(1, 6):
                cur.execute("""
                    INSERT INTO pools (pool_id, queue_id, min_friends) 
                    VALUES (%s, 0, %s) 
                    ON CONFLICT DO NOTHING
                """, (pid, m))
            print("OK")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"   ⚠️ Error en Bootstrap de Pools: {e}")

    # 1. Actualizar Usuarios
    print("\n[STEP 1/4] --- PASO 1: Actualizando índices de usuarios (Todas las pools) ---", flush=True)
    run_command(["src/extract/ingest_users.py", "--mode", "all"])

    # 2. Ingesta L0
    print("\n[STEP 2/4] --- PASO 2: Ingesta L0 (Nuevas partidas de Riot API) ---", flush=True)
    run_command(["src/extract/ingest_matches.py", "--limit", str(args_cli.limit)])

    # 3. Generar Pool Season (Especial)
    print("\n[STEP 3/4] --- PASO 3: Generando Pool Season (Fechas fijas) ---", flush=True)
    run_command(["src/pipeline.py", "--mode", "season", "--skip-l0", "--run-in-terminal"])

    # 4. Procesar cada Pool detectada
    print(f"\n[STEP 4/4] --- PASO 4: Generando tablas para {len(detected_pools)} Pools ---", flush=True)
    for i, pid in enumerate(detected_pools, 1):
        print(f"\n[{i}/{len(detected_pools)}] PROCESANDO POOL: {pid.upper()}", flush=True)
        
        # Determinar colección y parámetros
        if pid == "villaquesitos":
            users_coll = "L0_users_index"
            # Villaquesitos usa el modo l1-l2 estándar sin forzar pool_id (usa el hash)
            # PERO para consistencia, vamos a forzar su id técnico 'villaquesitos'
            pool_arg = "villaquesitos"
        else:
            users_coll = f"L0_users_index_{pid}"
            pool_arg = pid
        
        for m in range(1, 6):
            print(f"   >> Filtrando por min_friends={m} (ID: {pool_arg})...", flush=True)
            cmd = ["src/pipeline.py", "--mode", "l1-l2", "--min", str(m), "--skip-l0", "--run-in-terminal"]
            cmd.extend(["--pool", pool_arg])
            cmd.extend(["--users-collection", users_coll])
            run_command(cmd)

    print("\n" + "=" * 60)
    print("✅ PROCESO COMPLETADO EXITOSAMENTE")
    print("=" * 60)
    
    save_last_update()

if __name__ == "__main__":
    main()
