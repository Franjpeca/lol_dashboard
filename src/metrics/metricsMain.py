import sys
import subprocess
import os
import argparse
from dotenv import load_dotenv
from pymongo import MongoClient

sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")

def get_available_pools(queue, min_friends):
    """Busca en MongoDB todas las pools disponibles para la cola y min_friends dados."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    prefix = f"L1_q{queue}_min{min_friends}_pool_"
    
    pools = set()
    for name in db.list_collection_names():
        if name.startswith(prefix):
            # Ejemplo: L1_q440_min5_pool_ac89fa8d -> ac89fa8d
            parts = name.split("_pool_")
            if len(parts) > 1:
                pools.add(parts[1])
    
    client.close()
    return sorted(list(pools))

def check_l1_exists(queue, min_friends, pool_id):
    """Verifica si existe al menos una colección L1 para los parámetros dados."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    prefix = f"L1_q{queue}_min{min_friends}_"
    pool_tag = f"pool_{pool_id}"
    
    for name in db.list_collection_names():
        if name.startswith(prefix) and pool_tag in name:
            client.close()
            return True
    client.close()
    return False

def run_metric_script(script_name, args):
    # Construir ruta al script
    script_path = os.path.join(os.path.dirname(__file__), f"{script_name}.py")
    
    cmd = [sys.executable, script_path] + args
    print(f"   -> {script_name}...")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Fallo en {script_name}: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=440)
    parser.add_argument("--min", type=int, default=5)
    parser.add_argument("--pool", type=str, default=None, help="Si no se indica, se ejecuta para TODAS las pools encontradas.")
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    args = parser.parse_args()
    
    queue = args.queue
    min_friends = args.min
    
    # Determinar pools objetivo
    if args.pool:
        target_pools = [args.pool]
    else:
        print(f"[INIT] Buscando pools disponibles para q={queue} min={min_friends}...")
        target_pools = get_available_pools(queue, min_friends)
        if not target_pools:
            print("[WARN] No se encontraron pools.")
            return
        print(f"[INFO] Pools encontradas: {target_pools}")
    
    scripts_to_run = [
        "metrics_01_players_games_winrate", "metrics_02_champions_games_winrate",
        "metrics_03_games_frecuency", "metrics_04_win_lose_streak",
        "metrics_05_players_stats", "metrics_06_ego_index",
        "metrics_07_troll_index", "metrics_08_first_metrics",
        "metrics_09_number_skills", "metrics_10_stats_by_rol",
        "metrics_11_stats_record", "metrics_12_botlane_synergy",
        "metrics_13_player_champions_stats"
    ]
    
    for pool_id in target_pools:
        print(f"\n[POOL] Procesando pool: {pool_id}")

        if not check_l1_exists(queue, min_friends, pool_id):
            print(f"[WARN] No se detecta coleccion L1 para q={queue} min={min_friends} pool={pool_id}.")
            
            # Diagnostico: mostrar que colecciones SI existen para esa pool
            client = MongoClient(MONGO_URI)
            db = client[DB_NAME]
            found = [c for c in db.list_collection_names() if f"pool_{pool_id}" in c and c.startswith("L1_")]
            client.close()
            if found:
                print(f"       Colecciones disponibles para esta pool: {found}")
            continue
        
        # Argumentos para los scripts hijos
        child_args = ["--queue", str(queue), "--min", str(min_friends), "--pool", pool_id]
        if args.start: child_args += ["--start", args.start]
        if args.end: child_args += ["--end", args.end]
        
        for script in scripts_to_run:
            run_metric_script(script, child_args)

    print("\n[DONE] Orquestador de metricas finalizado")

if __name__ == "__main__":
    main()
