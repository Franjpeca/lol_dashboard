import json
import os
import argparse
from pathlib import Path
from collections import defaultdict
from pymongo import MongoClient
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Ruta base
BASE_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = BASE_DIR / "data" / "results"

# Conexión con MongoDB
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))
DEFAULT_POOL = "ac89fa8d"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
RESULTS_ROOT = Path("data/results")


def auto_select_l1(queue, min_friends):
    """Selecciona la colección L1 más reciente según los parámetros queue y min_friends."""
    prefix = f"L1_q{queue}_min{min_friends}_"
    cands = [c for c in db.list_collection_names() if c.startswith(prefix)]
    if not cands:
        return None
    cands.sort()
    return cands[-1]


def extract_pool_from_l1(l1_name):
    """Extrae el pool de un nombre de colección L1."""
    return "pool_" + l1_name.split("_pool_", 1)[1]

def process_stats(l1_collection):
    """Procesa estadísticas por rol y jugador desde una colección L1."""
    coll = db[l1_collection]
    
    # Cargar mapeo de puuid -> persona desde L0_users_index
    l0_coll = db["L0_users_index"]
    puuid_to_persona = {}
    for user in l0_coll.find({}, {"_id": 1, "persona": 1, "puuids": 1}):
        persona = user.get("persona", "Unknown")
        for puuid in user.get("puuids", []):
            puuid_to_persona[puuid] = persona
    
    # Estructura: role -> persona -> stats
    stats_by_role = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    games_by_role_player = defaultdict(lambda: defaultdict(int))
    kill_participation_by_role_player = defaultdict(lambda: defaultdict(list))
    
    cursor = coll.find({}, {"data.info.participants": 1, "friends_present": 1})
    
    for doc in cursor:
        participants = doc.get("data", {}).get("info", {}).get("participants", [])
        friends_present = doc.get("friends_present", [])
        
        # Calcular kills totales por equipo en este juego
        team_kills = defaultdict(int)
        for player in participants:
            team = player.get("teamId")
            team_kills[team] += player.get("kills", 0)
        
        for player in participants:
            puuid = player.get("puuid")
            
            # Solo procesar jugadores tracked
            if puuid not in friends_present:
                continue
            
            persona = puuid_to_persona.get(puuid, "Unknown")
            position = player.get("teamPosition", "UNKNOWN")
            
            games_by_role_player[position][persona] += 1
            stats_by_role[position][persona]["total_damage"] += player.get("totalDamageDealt", 0)
            stats_by_role[position][persona]["damage_taken"] += player.get("totalDamageTaken", 0)
            stats_by_role[position][persona]["gold"] += player.get("goldEarned", 0)
            stats_by_role[position][persona]["farm"] += player.get("totalMinionsKilled", 0)
            stats_by_role[position][persona]["vision"] += player.get("visionScore", 0)
            stats_by_role[position][persona]["turret_damage"] += player.get("damageDealtToBuildings", 0)
            stats_by_role[position][persona]["kills"] += player.get("kills", 0)
            stats_by_role[position][persona]["deaths"] += player.get("deaths", 0)
            stats_by_role[position][persona]["assists"] += player.get("assists", 0)
            stats_by_role[position][persona]["wins"] += (1 if player.get("win", False) else 0)
            
            # Calcular kill participation para este juego
            player_kills = player.get("kills", 0)
            player_assists = player.get("assists", 0)
            team_id = player.get("teamId")
            total_team_kills = team_kills.get(team_id, 1)
            
            if total_team_kills > 0:
                kill_participation = ((player_kills + player_assists) / total_team_kills) * 100
            else:
                kill_participation = 0
            
            kill_participation_by_role_player[position][persona].append(kill_participation)
    
    # Calcular promedios
    result = {}
    for position in stats_by_role:
        result[position] = {}
        for persona in stats_by_role[position]:
            games = games_by_role_player[position][persona]
            if games > 0:
                # Calcular promedio de kill participation
                kp_list = kill_participation_by_role_player[position][persona]
                avg_kill_participation = round(sum(kp_list) / len(kp_list), 2) if kp_list else 0
                
                result[position][persona] = {
                    "games": games,
                    "avg_damage": round(stats_by_role[position][persona]["total_damage"] / games, 2),
                    "avg_damage_taken": round(stats_by_role[position][persona]["damage_taken"] / games, 2),
                    "avg_gold": round(stats_by_role[position][persona]["gold"] / games, 2),
                    "avg_farm": round(stats_by_role[position][persona]["farm"] / games, 2),
                    "avg_vision": round(stats_by_role[position][persona]["vision"] / games, 2),
                    "avg_turret_damage": round(stats_by_role[position][persona]["turret_damage"] / games, 2),
                    "avg_kills": round(stats_by_role[position][persona]["kills"] / games, 2),
                    "avg_deaths": round(stats_by_role[position][persona]["deaths"] / games, 2),
                    "avg_assists": round(stats_by_role[position][persona]["assists"] / games, 2),
                    "avg_kill_participation": avg_kill_participation,
                    "winrate": round((stats_by_role[position][persona]["wins"] / games) * 100, 2),
                }
    
    return result


def save_stats(stats, dataset_folder):
    """Guarda las estadísticas en un archivo JSON."""
    out_path = dataset_folder / "metrics_10_stats_by_rol.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE)
    parser.add_argument("--min", type=int, default=DEFAULT_MIN)
    args = parser.parse_args()

    queue = args.queue
    min_friends = args.min
    pool_id = DEFAULT_POOL

    print(f"[10] Starting ... using collection: L1_q{queue}_min{min_friends}")
    
    l1_name = auto_select_l1(queue, min_friends)
    if not l1_name:
        return

    pool_id = extract_pool_from_l1(l1_name)

    dataset_folder = RESULTS_ROOT / pool_id / f"q{queue}" / f"min{min_friends}"
    dataset_folder.mkdir(parents=True, exist_ok=True)

    stats = process_stats(l1_name)
    save_stats(stats, dataset_folder)
    
    print(f"[10] Ended")


def run():
    main()


if __name__ == "__main__":
    run()
