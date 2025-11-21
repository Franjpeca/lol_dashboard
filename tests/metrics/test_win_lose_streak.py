import json
from pathlib import Path

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

###############################################################################
# M04 - WIN / LOSE STREAK
###############################################################################

def test_m04_files_exist():
    m04_files = find_all("metrics_04_win_lose_streak.json")
    assert len(m04_files) > 0, "[M04] ERROR: no files found"
    for file in m04_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M04] OK file={rel} found")

def test_m04_structure_and_types():
    m04_files = find_all("metrics_04_win_lose_streak.json")
    for file in m04_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M04] Checking structure file={rel}")
        data = load_json(file)

        assert isinstance(data, dict), f"[M04] ERROR file={rel}: root not dict"
        assert len(data) > 0, f"[M04] ERROR file={rel}: empty dict"

        for persona, streaks in data.items():
            assert isinstance(persona, str), f"[M04] ERROR file={rel}: persona not str"
            assert isinstance(streaks, dict), f"[M04] ERROR file={rel}: streak data not dict"

            assert "max_win_streak" in streaks, f"[M04] ERROR file={rel}: max_win_streak missing"
            assert "max_lose_streak" in streaks, f"[M04] ERROR file={rel}: max_lose_streak missing"
            assert "current_streak" in streaks, f"[M04] ERROR file={rel}: current_streak missing"

            assert isinstance(streaks["max_win_streak"], int), f"[M04] ERROR file={rel}: max_win_streak not int"
            assert isinstance(streaks["max_lose_streak"], int), f"[M04] ERROR file={rel}: max_lose_streak not int"
            assert isinstance(streaks["current_streak"], int), f"[M04] ERROR file={rel}: current_streak not int"

        print(f"[M04] OK file={rel} structure")

def test_m04_ranges():
    m04_files = find_all("metrics_04_win_lose_streak.json")
    for file in m04_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M04] Checking ranges file={rel}")
        data = load_json(file)

        for persona, streaks in data.items():
            mws = streaks["max_win_streak"]
            mls = streaks["max_lose_streak"]
            cs = streaks["current_streak"]

            assert mws >= 0, f"[M04] ERROR file={rel}: max_win_streak negative"
            assert mls >= 0, f"[M04] ERROR file={rel}: max_lose_streak negative"

            assert abs(cs) <= 1000, f"[M04] ERROR file={rel}: current_streak absurdly large"
            assert mws <= 1000, f"[M04] ERROR file={rel}: max_win_streak absurdly large"
            assert mls <= 1000, f"[M04] ERROR file={rel}: max_lose_streak absurdly large"

        print(f"[M04] OK file={rel} ranges")

def test_m04_internal_consistency():
    m04_files = find_all("metrics_04_win_lose_streak.json")
    for file in m04_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M04] Checking internal consistency file={rel}")
        data = load_json(file)

        for persona, streaks in data.items():
            mws = streaks["max_win_streak"]
            mls = streaks["max_lose_streak"]
            cs = streaks["current_streak"]

            if cs > 0:
                assert cs <= mws, f"[M04] ERROR file={rel}: current_streak > max_win_streak"
            if cs < 0:
                assert abs(cs) <= mls, f"[M04] ERROR file={rel}: abs(current_streak) > max_lose_streak"

            if mws == 0:
                assert cs <= 0, f"[M04] ERROR file={rel}: max_win_streak=0 but current_streak positive"
            if mls == 0:
                assert cs >= 0, f"[M04] ERROR file={rel}: max_lose_streak=0 but current_streak negative"

        print(f"[M04] OK file={rel} internal consistency")

def test_m04_persona_name_validity_against_m01():
    m04_files = find_all("metrics_04_win_lose_streak.json")
    m01_files = find_all("metrics_01_players_games_winrate.json")

    if len(m01_files) == 0:
        for file in m04_files:
            rel = file.relative_to(Path("data/results"))
            print(f"[M04] SKIP file={rel}: no M01 available for name validation")
        return

    m01_personas = set()
    for f in m01_files:
        d = load_json(f)
        for u in d["users"]:
            m01_personas.add(u["persona"])

    for file in m04_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M04] Checking persona names file={rel}")
        data = load_json(file)

        for persona in data.keys():
            assert persona in m01_personas, f"[M04] ERROR file={rel}: persona not found in M01"

        print(f"[M04] OK file={rel} persona names match M01")
