import json
from pathlib import Path
from datetime import datetime

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

###############################################################################
# M03 - GAMES FREQUENCY
###############################################################################

def test_m03_files_exist():
    m03_files = find_all("metrics_03_games_frecuency.json")
    assert len(m03_files) > 0, "[M03] ERROR: no files found"
    for file in m03_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M03] OK file={rel} found")

def test_m03_global_structure():
    m03_files = find_all("metrics_03_games_frecuency.json")
    for file in m03_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M03] Checking global_frequency structure file={rel}")
        data = load_json(file)

        assert "global_frequency" in data, f"[M03] ERROR file={rel}: global_frequency missing"
        gf = data["global_frequency"]
        assert isinstance(gf, list), f"[M03] ERROR file={rel}: global_frequency not list"
        assert len(gf) > 0, f"[M03] ERROR file={rel}: global_frequency empty"

        for entry in gf:
            assert isinstance(entry, dict), f"[M03] ERROR file={rel}: entry not dict"
            assert "date" in entry, f"[M03] ERROR file={rel}: date missing"
            assert "games" in entry, f"[M03] ERROR file={rel}: games missing"
            assert isinstance(entry["date"], str), f"[M03] ERROR file={rel}: date not str"
            assert isinstance(entry["games"], int), f"[M03] ERROR file={rel}: games not int"
            assert entry["games"] >= 0, f"[M03] ERROR file={rel}: negative games"

        print(f"[M03] OK file={rel} global_frequency structure")

def test_m03_global_dates_valid_and_ordered():
    m03_files = find_all("metrics_03_games_frecuency.json")
    for file in m03_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M03] Checking global_frequency dates file={rel}")
        data = load_json(file)
        gf = data["global_frequency"]

        dates = [entry["date"] for entry in gf]
        parsed = []

        for d in dates:
            try:
                parsed.append(datetime.strptime(d, "%Y-%m-%d"))
            except:
                assert False, f"[M03] ERROR file={rel}: invalid date format {d}"

        assert len(parsed) == len(set(parsed)), f"[M03] ERROR file={rel}: duplicate dates"

        assert parsed == sorted(parsed), f"[M03] ERROR file={rel}: dates not ordered"

        print(f"[M03] OK file={rel} global_frequency dates")

def test_m03_global_total_nonzero():
    m03_files = find_all("metrics_03_games_frecuency.json")
    for file in m03_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M03] Checking global total file={rel}")
        data = load_json(file)
        total = sum([entry["games"] for entry in data["global_frequency"]])
        assert total >= 0, f"[M03] ERROR file={rel}: negative total"
        print(f"[M03] OK file={rel} total={total}")

def test_m03_players_structure():
    m03_files = find_all("metrics_03_games_frecuency.json")
    for file in m03_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M03] Checking players_frequency structure file={rel}")
        data = load_json(file)

        assert "players_frequency" in data, f"[M03] ERROR file={rel}: players_frequency missing"
        pf = data["players_frequency"]
        assert isinstance(pf, list), f"[M03] ERROR file={rel}: players_frequency not list"

        for player in pf:
            assert "persona" in player, f"[M03] ERROR file={rel}: persona missing"
            assert "total_games" in player, f"[M03] ERROR file={rel}: total_games missing"
            assert "games" in player, f"[M03] ERROR file={rel}: games list missing"

            assert isinstance(player["persona"], str), f"[M03] ERROR file={rel}: persona not str"
            assert isinstance(player["total_games"], int), f"[M03] ERROR file={rel}: total_games not int"

            games_list = player["games"]
            assert isinstance(games_list, list), f"[M03] ERROR file={rel}: games not list"

            for entry in games_list:
                assert "date" in entry, f"[M03] ERROR file={rel}: game entry missing date"
                assert "games" in entry, f"[M03] ERROR file={rel}: game entry missing games"
                assert isinstance(entry["date"], str), f"[M03] ERROR file={rel}: date not str"
                assert isinstance(entry["games"], int), f"[M03] ERROR file={rel}: games not int"
                assert entry["games"] >= 0, f"[M03] ERROR file={rel}: negative games in games list"

        print(f"[M03] OK file={rel} players_frequency structure")

def test_m03_players_games_sum_consistency():
    m03_files = find_all("metrics_03_games_frecuency.json")

    for file in m03_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M03] Checking players total consistency file={rel}")
        data = load_json(file)

        for player in data["players_frequency"]:
            games_list = player["games"]
            total_list = sum([g["games"] for g in games_list])
            total_user = player["total_games"]

            assert total_user >= 0, f"[M03] ERROR file={rel}: total_games negative"

            if total_user > 0:
                assert total_list == total_user, f"[M03] ERROR file={rel}: total_games mismatch"

        print(f"[M03] OK file={rel} players total consistency")

def test_m03_global_vs_player_totals():
    m03_files = find_all("metrics_03_games_frecuency.json")

    for file in m03_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M03] Comparing global and player totals file={rel}")
        data = load_json(file)

        total_global = sum([g["games"] for g in data["global_frequency"]])
        total_players = sum([p["total_games"] for p in data["players_frequency"]])

        assert total_global <= total_players, f"[M03] ERROR file={rel}: global > players sum"

        print(f"[M03] OK file={rel} total_global={total_global} <= total_players={total_players}")
