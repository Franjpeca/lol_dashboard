import json
from pathlib import Path

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def almost_equal(a, b, tol=1e-9):
    return abs(a - b) <= tol

###############################################################################
# M01 - PLAYERS WINRATE
###############################################################################

def test_m01_all_files_structure():
    m01_files = find_all("metrics_01_players_games_winrate.json")

    assert len(m01_files) > 0, "[M01] ERROR: no files found for metrics_01"

    for file in m01_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M01] Checking structure file={rel}")
        data = load_json(file)

        assert "users" in data, f"[M01] ERROR file={rel}: users missing"
        assert isinstance(data["users"], list), f"[M01] ERROR file={rel}: users not list"
        assert len(data["users"]) > 0, f"[M01] ERROR file={rel}: users empty"

        print(f"[M01] OK file={rel} structure")

def test_m01_all_files_user_fields_and_types():
    m01_files = find_all("metrics_01_players_games_winrate.json")

    for file in m01_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M01] Checking user fields file={rel}")
        data = load_json(file)

        for user in data["users"]:
            assert "persona" in user, f"[M01] ERROR file={rel}: persona missing"
            assert "total_games" in user, f"[M01] ERROR file={rel}: total_games missing"
            assert "total_wins" in user, f"[M01] ERROR file={rel}: total_wins missing"
            assert "winrate" in user, f"[M01] ERROR file={rel}: winrate missing"
            assert "puuids" in user, f"[M01] ERROR file={rel}: puuids missing"

            assert isinstance(user["persona"], str), f"[M01] ERROR file={rel}: persona not str"
            assert isinstance(user["total_games"], int), f"[M01] ERROR file={rel}: total_games not int"
            assert isinstance(user["total_wins"], int), f"[M01] ERROR file={rel}: total_wins not int"
            assert isinstance(user["winrate"], float), f"[M01] ERROR file={rel}: winrate not float"
            assert isinstance(user["puuids"], dict), f"[M01] ERROR file={rel}: puuids not dict"

        print(f"[M01] OK file={rel} user fields")

def test_m01_all_files_values_and_math():
    m01_files = find_all("metrics_01_players_games_winrate.json")

    for file in m01_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M01] Checking math file={rel}")
        data = load_json(file)

        for user in data["users"]:
            games = user["total_games"]
            wins = user["total_wins"]
            wr = user["winrate"]

            assert games >= 0, f"[M01] ERROR file={rel}: games negative"
            assert wins >= 0, f"[M01] ERROR file={rel}: wins negative"
            assert wins <= games, f"[M01] ERROR file={rel}: wins > games"
            assert 0 <= wr <= 1, f"[M01] ERROR file={rel}: winrate out of range"

            if games > 0:
                assert almost_equal(wr, wins / games), f"[M01] ERROR file={rel}: winrate mismatch"

        print(f"[M01] OK file={rel} winrate math")

def test_m01_all_files_puuid_consistency():
    m01_files = find_all("metrics_01_players_games_winrate.json")

    for file in m01_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M01] Checking puuid consistency file={rel}")
        data = load_json(file)

        for user in data["users"]:
            sum_games = 0
            sum_wins = 0

            for entry in user["puuids"].values():
                g = entry["games"]
                w = entry["wins"]
                wr = entry["winrate"]

                assert g >= 0, f"[M01] ERROR file={rel}: puuid games negative"
                assert w >= 0, f"[M01] ERROR file={rel}: puuid wins negative"
                assert w <= g, f"[M01] ERROR file={rel}: puuid wins > games"
                assert 0 <= wr <= 1, f"[M01] ERROR file={rel}: puuid winrate out of range"

                if g > 0:
                    assert almost_equal(wr, w / g), f"[M01] ERROR file={rel}: puuid winrate mismatch"

                sum_games += g
                sum_wins += w

            assert sum_games == user["total_games"], f"[M01] ERROR file={rel}: total_games mismatch"
            assert sum_wins == user["total_wins"], f"[M01] ERROR file={rel}: total_wins mismatch"

        print(f"[M01] OK file={rel} puuid consistency")

###############################################################################
# M02 - CHAMPIONS WINRATE
###############################################################################

def test_m02_all_files_structure():
    m02_files = find_all("metrics_02_champions_games_winrate.json")

    assert len(m02_files) > 0, "[M02] ERROR: no files found for metrics_02"

    for file in m02_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M02] Checking structure file={rel}")
        data = load_json(file)

        assert "champions" in data, f"[M02] ERROR file={rel}: champions missing"
        assert "enemy_champions" in data, f"[M02] ERROR file={rel}: enemy_champions missing"
        assert isinstance(data["champions"], list), f"[M02] ERROR file={rel}: champions not list"
        assert isinstance(data["enemy_champions"], list), f"[M02] ERROR file={rel}: enemy_champions not list"
        assert len(data["champions"]) > 0, f"[M02] ERROR file={rel}: champions empty"
        assert len(data["enemy_champions"]) > 0, f"[M02] ERROR file={rel}: enemy_champions empty"

        print(f"[M02] OK file={rel} structure")

def test_m02_all_files_champion_fields_and_types():
    m02_files = find_all("metrics_02_champions_games_winrate.json")

    for file in m02_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M02] Checking champion fields file={rel}")
        data = load_json(file)

        for section in ["champions", "enemy_champions"]:
            for entry in data[section]:
                assert "champion" in entry, f"[M02] ERROR file={rel}: champion missing in {section}"
                assert "games" in entry, f"[M02] ERROR file={rel}: games missing in {section}"
                assert "winrate" in entry, f"[M02] ERROR file={rel}: winrate missing in {section}"
                assert isinstance(entry["champion"], str), f"[M02] ERROR file={rel}: champion not str"
                assert isinstance(entry["games"], int), f"[M02] ERROR file={rel}: games not int"
                assert isinstance(entry["winrate"], (int, float)), f"[M02] ERROR file={rel}: winrate not number"

        print(f"[M02] OK file={rel} champion fields")

def test_m02_all_files_values_ranges():
    m02_files = find_all("metrics_02_champions_games_winrate.json")

    for file in m02_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M02] Checking value ranges file={rel}")
        data = load_json(file)

        for section in ["champions", "enemy_champions"]:
            for entry in data[section]:
                g = entry["games"]
                wr = entry["winrate"]

                assert g >= 0, f"[M02] ERROR file={rel}: games negative in {section}"
                assert 0 <= wr <= 100, f"[M02] ERROR file={rel}: winrate out of range in {section}"

                if g == 0:
                    assert wr == 0, f"[M02] ERROR file={rel}: winrate nonzero with 0 games"

        print(f"[M02] OK file={rel} value ranges")

def test_m02_all_files_no_duplicates():
    m02_files = find_all("metrics_02_champions_games_winrate.json")

    for file in m02_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M02] Checking duplicates file={rel}")
        data = load_json(file)

        champs = [c["champion"] for c in data["champions"]]
        enemy_champs = [c["champion"] for c in data["enemy_champions"]]

        assert len(champs) == len(set(champs)), f"[M02] ERROR file={rel}: duplicate in champions"
        assert len(enemy_champs) == len(set(enemy_champs)), f"[M02] ERROR file={rel}: duplicate in enemy_champions"

        print(f"[M02] OK file={rel} no duplicates")
