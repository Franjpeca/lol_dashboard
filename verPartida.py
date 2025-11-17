import os
import json
import argparse
from pathlib import Path
from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime

# Cargar variables de entorno
load_dotenv()

# Conexión a MongoDB
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
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


def format_game_duration(seconds):
    """Convierte segundos a formato MM:SS"""
    if seconds == 0:
        return "0m 0s"
    minutes = seconds // 60
    seconds = seconds % 60
    return f"{minutes}m {seconds}s"


def get_game_details(game_id, queue, min_friends):
    """Buscar los detalles de la partida en MongoDB usando el game_id."""
    # Seleccionar la colección L1 correspondiente según los parámetros
    l1_name = auto_select_l1(queue, min_friends)
    if not l1_name:
        raise Exception(f"No se encontró la colección L1 correspondiente para queue {queue} y min_friends {min_friends}")
    
    coll = db[l1_name]
    
    # Realizar la consulta para obtener la partida por game_id
    game = coll.find_one({"data.info.gameId": game_id})
    
    if game:
        # Extraer la información relevante
        game_info = game.get("data", {}).get("info", {})
        participants = game_info.get("participants", [])
        game_date = datetime.fromtimestamp(game_info.get("gameCreation", 0) / 1000)  # Convertir a fecha legible
        game_duration = game_info.get("gameDuration", 0)  # En segundos
        team_1 = [p for p in participants if p.get("teamId") == 100]
        team_2 = [p for p in participants if p.get("teamId") == 200]
        
        # Recoger la información de jugadores y campeones, y si ganaron o perdieron
        team_1_info = [{"name": p.get("riotIdGameName", "Unknown"), 
                        "champion": p.get("championName", "Unknown"), 
                        "kills": p.get("kills", 0),
                        "deaths": p.get("deaths", 0),
                        "assists": p.get("assists", 0),
                        "win": p.get("win", False)} for p in team_1]
        team_2_info = [{"name": p.get("riotIdGameName", "Unknown"), 
                        "champion": p.get("championName", "Unknown"), 
                        "kills": p.get("kills", 0),
                        "deaths": p.get("deaths", 0),
                        "assists": p.get("assists", 0),
                        "win": p.get("win", False)} for p in team_2]

        # Calcular las kills globales por equipo
        team_1_kills = sum(p['kills'] for p in team_1)
        team_2_kills = sum(p['kills'] for p in team_2)

        # Determinar si el equipo 1 ganó o perdió
        team_1_result = "Ganó" if any(p['win'] for p in team_1) else "Perdió"
        # Determinar si el equipo 2 ganó o perdió
        team_2_result = "Ganó" if any(p['win'] for p in team_2) else "Perdió"

        return {
            "game_id": game_id,
            "game_date": game_date.strftime("%Y-%m-%d %H:%M:%S"),  # Fecha en formato legible
            "game_duration": format_game_duration(game_duration),  # Duración formateada
            "team_1": {
                "players": team_1_info,
                "result": team_1_result,
                "total_kills": team_1_kills
            },
            "team_2": {
                "players": team_2_info,
                "result": team_2_result,
                "total_kills": team_2_kills
            }
        }
    else:
        return None


def main():
    parser = argparse.ArgumentParser(description="Buscar detalles de una partida en MongoDB")
    parser.add_argument("--id", type=int, required=True, help="ID de la partida (game_id)")
    parser.add_argument("--queue", type=int, default=440, help="ID de la cola (por defecto 440)")
    parser.add_argument("--min_friends", type=int, default=5, help="Número mínimo de amigos (por defecto 5)")
    
    args = parser.parse_args()

    game_id = args.id
    queue = args.queue
    min_friends = args.min_friends

    print(f"Buscando detalles para la partida con game_id {game_id}, cola {queue}, mínimo de amigos {min_friends}...")

    try:
        game_details = get_game_details(game_id, queue, min_friends)

        if game_details:
            print(f"Detalles de la partida:")
            print(f"ID de la partida: {game_details['game_id']}")
            print(f"Fecha de la partida: {game_details['game_date']}")
            print(f"Duración de la partida: {game_details['game_duration']}")
            print(f"\nEquipo 1 (Tu equipo) - Resultado: {game_details['team_1']['result']}, Kills globales: {game_details['team_1']['total_kills']}")
            for player in game_details["team_1"]["players"]:
                print(f"Jugador: {player['name']}, Campeón: {player['champion']}, Kills: {player['kills']}, Muertes: {player['deaths']}, Asistencias: {player['assists']}")

            print(f"\nEquipo 2 (Rival) - Resultado: {game_details['team_2']['result']}, Kills globales: {game_details['team_2']['total_kills']}")
            for player in game_details["team_2"]["players"]:
                print(f"Jugador: {player['name']}, Campeón: {player['champion']}, Kills: {player['kills']}, Muertes: {player['deaths']}, Asistencias: {player['assists']}")
        else:
            print(f"No se encontró la partida con el game_id: {game_id}")
    
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
