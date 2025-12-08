import json
from collections import defaultdict

def check_botlane_synergy(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    total_games = 0
    total_wins = 0
    issues = []
    duo_counts = defaultdict(int)

    # Suponemos que los duos están en una lista bajo alguna clave
    # Buscamos recursivamente diccionarios con keys 'duo', 'games', 'wins', 'winrate'
    def recurse(obj, path="root"):
        nonlocal total_games, total_wins
        if isinstance(obj, dict):
            if 'duo' in obj and 'games' in obj:
                g = obj.get('games')
                w = obj.get('wins')
                wr = obj.get('winrate')
                duo = tuple(obj.get('duo'))
                duo_counts[duo] += 1

                # Sumar games / wins si son ints
                if isinstance(g, int):
                    total_games += g
                else:
                    issues.append((path + ".games", g))

                if isinstance(w, int):
                    total_wins += w
                else:
                    issues.append((path + ".wins", w))

                # Check winrate coherencia
                if isinstance(g, int) and g > 0 and isinstance(w, int):
                    calc_wr = w / g
                    # Tolerancia pequeña porque winrate puede ser float
                    if abs(calc_wr - wr) > 1e-6:
                        issues.append((path + ".winrate incoherente", {"games": g, "wins": w, "winrate": wr, "calc": calc_wr}))

            for k, v in obj.items():
                recurse(v, path + "." + k)
        elif isinstance(obj, list):
            for idx, item in enumerate(obj):
                recurse(item, path + f"[{idx}]")

    recurse(data)

    print("Suma de games:", total_games)
    print("Suma de wins:", total_wins)

    if total_wins > total_games:
        print("❌ ERROR: suma de wins es mayor que suma de games!")

    # Mostrar duos repetidos
    repeated = {duo: cnt for duo, cnt in duo_counts.items() if cnt > 1}
    if repeated:
        print("⚠️ Algunos duos están repetidos (posible duplicación de partidas):")
        for duo, cnt in repeated.items():
            print(f"  Duo {duo} aparece {cnt} veces")

    if issues:
        print("Problemas detectados en entradas:")
        for p, val in issues:
            print("  ", p, ":", val)
    else:
        print("Todos los checks básicos pasaron OK.")

if __name__ == '__main__':
    file_path = r"C:\Users\Diazr\Documents\ficheros_escritorio\lol_data\data\results\pool_ac89fa8d\q440\min5\metrics_12_botlane_synergy.json"
    check_botlane_synergy(file_path)
