import json
from pathlib import Path

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

###############################################################################
# M06 - EGO INDEX
###############################################################################

def test_m06_files_exist():
    m06_files = find_all("metrics_06_ego_index.json")
    assert len(m06_files) > 0, "[M06] ERROR: no files found"
    for file in m06_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M06] OK file={rel} found")

def test_m06_structure_and_types():
    m06_files = find_all("metrics_06_ego_index.json")
    for file in m06_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M06] Checking structure file={rel}")
        data = load_json(file)

        for persona, stats in data.items():
            assert isinstance(persona, str), f"[M06] ERROR file={rel}: persona not str"
            assert isinstance(stats, dict), f"[M06] ERROR file={rel}: stats not dict"

            required = [
                "ego_index", "selfish_score", "teamplay_score", "tilt_score",
                "match_count", "avg_kills", "avg_deaths", "avg_assists",
                "avg_gold", "avg_damage_dealt", "avg_vision_score",
                "lost_by_surrender", "lost_surrender_rate"
            ]

            for key in required:
                assert key in stats, f"[M06] ERROR file={rel}: {key} missing"

            for key in [
                "ego_index", "selfish_score", "teamplay_score", "tilt_score",
                "avg_kills", "avg_deaths", "avg_assists", "avg_gold",
                "avg_damage_dealt", "avg_vision_score",
                "lost_surrender_rate"
            ]:
                assert isinstance(stats[key], (int, float)), f"[M06] ERROR file={rel}: {key} not number"

            assert isinstance(stats["match_count"], int), f"[M06] ERROR file={rel}: match_count not int"
            assert isinstance(stats["lost_by_surrender"], int), f"[M06] ERROR file={rel}: lost_by_surrender not int"

        print(f"[M06] OK file={rel} structure")

def test_m06_ranges():
    m06_files = find_all("metrics_06_ego_index.json")
    for file in m06_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M06] Checking ranges file={rel}")
        data = load_json(file)

        for persona, s in data.items():
            assert -5 <= s["ego_index"] <= 5, f"[M06] ERROR file={rel}: ego_index out of range"

            for key in ["selfish_score", "teamplay_score", "tilt_score"]:
                assert 0 <= s[key] <= 5, f"[M06] ERROR file={rel}: {key} out of range"

            assert 0 <= s["avg_kills"] <= 40, f"[M06] ERROR file={rel}: avg_kills invalid"
            assert 0 <= s["avg_deaths"] <= 40, f"[M06] ERROR file={rel}: avg_deaths invalid"
            assert 0 <= s["avg_assists"] <= 50, f"[M06] ERROR file={rel}: avg_assists invalid"
            assert 0 <= s["avg_gold"] <= 30000, f"[M06] ERROR file={rel}: avg_gold invalid"
            assert 0 <= s["avg_damage_dealt"] <= 200000, f"[M06] ERROR file={rel}: avg_damage_dealt invalid"
            assert 0 <= s["avg_vision_score"] <= 200, f"[M06] ERROR file={rel}: avg_vision_score invalid"

            assert s["match_count"] >= 0, f"[M06] ERROR file={rel}: match_count negative"
            assert s["lost_by_surrender"] >= 0, f"[M06] ERROR file={rel}: lost_by_surrender negative"
            assert s["lost_by_surrender"] <= s["match_count"], f"[M06] ERROR file={rel}: surrenders > matches"

        print(f"[M06] OK file={rel} ranges")

def test_m06_surrender_rate_math():
    m06_files = find_all("metrics_06_ego_index.json")
    for file in m06_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M06] Checking surrender rate math file={rel}")
        data = load_json(file)

        for persona, s in data.items():
            mc = s["match_count"]
            ls = s["lost_by_surrender"]
            rate = s["lost_surrender_rate"]

            if mc == 0:
                assert rate == 0, f"[M06] ERROR file={rel}: surrender_rate nonzero with match_count=0"
            else:
                expected = ls / mc
                assert abs(rate - expected) < 1e-6, f"[M06] ERROR file={rel}: surrender_rate mismatch"

        print(f"[M06] OK file={rel} surrender rate math")

def test_m06_personas_match_m01():
    m06_files = find_all("metrics_06_ego_index.json")
    m01_files = find_all("metrics_01_players_games_winrate.json")

    if len(m01_files) == 0:
        for file in m06_files:
            rel = file.relative_to(Path("data/results"))
            print(f"[M06] SKIP file={rel}: no M01 available")
        return

    m01_personas = set()
    for f in m01_files:
        d = load_json(f)
        for u in d["users"]:
            m01_personas.add(u["persona"])

    for file in m06_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M06] Checking personas vs M01 file={rel}")
        data = load_json(file)

        for persona in data.keys():
            assert persona in m01_personas, f"[M06] ERROR file={rel}: persona not in M01"

        print(f"[M06] OK file={rel} personas in M01")
