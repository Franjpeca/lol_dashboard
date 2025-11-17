import sys

from metrics_01_players_games_winrate import main as run_winrate_players
from metrics_02_champions_games_winrate import main as run_champions_winrate
from metrics_03_games_frecuency import main as run_games_frecuency
from metrics_04_win_lose_streak import main as run_win_lose_streak
from metrics_05_players_stats import main as run_players_stats
from metrics_06_ego_index import main as run_ego_index
from metrics_07_troll_index import main as run_troll_index
from metrics_08_first_metrics import main as run_first_metrics
from metrics_09_number_skills import main as run_number_skill
from metrics_10_stats_by_rol import main as run_stats_by_rol
from metrics_11_stats_record import main as run_record_stats

def main():
    print("[INIT] Inicio del orquestador de metricas")

    print("[INFO] Ejecutando métricas por jugador...")
    run_winrate_players()

    print("[INFO] Ejecutando métricas globales de campeones...")
    run_champions_winrate()

    print("[INFO] Ejecutando frecuencias de partidas...")
    run_games_frecuency()

    print("[INFO] Ejecutando rachas de victorias y derrotas...")
    run_win_lose_streak()

    print("[INFO] Ejecutando estadisticas de jugadores...")
    run_players_stats()

    print("[INFO] Ejecutando indice de ego...")
    run_ego_index()

    print("[INFO] Ejecutando indice de troll...")
    run_troll_index()

    print("[INFO] Ejecutando primeras estadisticas...")
    run_first_metrics()

    print("[INFO] Ejecutando numero de habilidades usadas...")
    run_number_skill()

    print("[INFO] Ejecutando estadisticas por rol...")
    run_stats_by_rol()

    print("[INFO] Ejecutando estadisticas de records...")
    run_record_stats()



    print("[DONE] Orquestador de metricas finalizado")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[ERROR] Fallo en el orquestador de metricas")
        print(str(e))
        sys.exit(1)
