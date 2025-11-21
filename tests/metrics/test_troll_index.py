import json
from pathlib import Path

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

###############################################################################
# M07 - TROLL INDEX
###############################################################################

def test_m07_files_exist():
    m07_files = find_all("metrics_07_troll_index.json")
    assert len(m07_files) > 0, "[M07] ERROR: no files found"
    for file in m07_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M07] OK file={rel} found")

def test_m07_structure_and_types():
    m07_files = find_all("metrics_07_troll_index.json")
    for file in m07_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M07] Checking structure file={rel}")
        data = load_json(file)

        for persona, stats in data.items():
            assert isinstance(persona, str), f"[M07] ERROR file={rel}: persona not str"
            assert isinstance(stats, dict), f"[M07] ERROR file={rel}: stats not dict"

            required = [
                "total_matches",
                "early_surrender_own", "early_surrender_enemy",
                "afk_own", "afk_enemy",
                "pct_early_surrender_own", "pct_early_surrender_enemy",
                "pct_afk_own", "pct_afk_enemy"
            ]
            for key in required:
                assert key in stats, f"[M07] ERROR file={rel}: {key} missing"

            assert isinstance(stats["total_matches"], int), f"[M07] ERROR file={rel}: total_matches not int"

            for key in [
                "early_surrender_own", "early_surrender_enemy",
                "afk_own", "afk_enemy"
            ]:
                assert isinstance(stats[key], int), f"[M07] ERROR file={rel}: {key} not int"

            for key in [
                "pct_early_surrender_own", "pct_early_surrender_enemy",
                "pct_afk_own", "pct_afk_enemy"
            ]:
                assert isinstance(stats[key], (int, float)), f"[M07] ERROR file={rel}: {key} not number"

        print(f"[M07] OK file={rel} structure")

def test_m07_ranges_and_limits():
    m07_files = find_all("metrics_07_troll_index.json")
    for file in m07_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M07] Checking ranges file={rel}")
        data = load_json(file)

        for persona, s in data.items():
            tm = s["total_matches"]

            assert tm >= 0, f"[M07] ERROR file={rel}: total_matches negative"

            for key in ["early_surrender_own", "early_surrender_enemy", "afk_own", "afk_enemy"]:
                assert 0 <= s[key] <= tm, f"[M07] ERROR file={rel}: {key} > total_matches"

            for key in [
                "pct_early_surrender_own", "pct_early_surrender_enemy",
                "pct_afk_own", "pct_afk_enemy"
            ]:
                assert 0 <= s[key] <= 1, f"[M07] ERROR file={rel}: {key} out of range"

        print(f"[M07] OK file={rel} ranges")

def test_m07_percentage_math():
    m07_files = find_all("metrics_07_troll_index.json")
    for file in m07_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M07] Checking percentage math file={rel}")
        data = load_json(file)

        for persona, s in data.items():
            tm = s["total_matches"]
            eo = s["early_surrender_own"]
            ee = s["early_surrender_enemy"]
            ao = s["afk_own"]
            ae = s["afk_enemy"]

            if tm == 0:
                assert s["pct_early_surrender_own"] == 0, f"[M07] ERROR file={rel}: pct_own nonzero with tm=0"
                assert s["pct_early_surrender_enemy"] == 0, f"[M07] ERROR file={rel}: pct_enemy nonzero with tm=0"
                assert s["pct_afk_own"] == 0, f"[M07] ERROR file={rel}: pct_afk_own nonzero with tm=0"
                assert s["pct_afk_enemy"] == 0, f"[M07] ERROR file={rel}: pct_afk_enemy nonzero with tm=0"
            else:
                assert abs(s["pct_early_surrender_own"] - eo / tm) < 1e-6, f"[M07] ERROR file={rel}: pct_own mismatch"
                assert abs(s["pct_early_surrender_enemy"] - ee / tm) < 1e-6, f"[M07] ERROR file={rel}: pct_enemy mismatch"
                assert abs(s["pct_afk_own"] - ao / tm) < 1e-6, f"[M07] ERROR file={rel}: pct_afk_own mismatch"
                assert abs(s["pct_afk_enemy"] - ae / tm) < 1e-6, f"[M07] ERROR file={rel}: pct_afk_enemy mismatch"

        print(f"[M07] OK file={rel} percentage math")

def test_m07_personas_match_m01():
    m07_files = find_all("metrics_07_troll_index.json")
    m01_files = find_all("metrics_01_players_games_winrate.json")

    if len(m01_files) == 0:
        for file in m07_files:
            rel = file.relative_to(Path("data/results"))
            print(f"[M07] SKIP file={rel}: no M01 available")
        return

    m01_personas = set()
    for f in m01_files:
        d = load_json(f)
        for u in d["users"]:
            m01_personas.add(u["persona"])

    for file in m07_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M07] Checking personas vs M01 file={rel}")
        data = load_json(file)

        for persona in data.keys():
            assert persona in m01_personas, f"[M07] ERROR file={rel}: persona not in M01"

        print(f"[M07] OK file={rel} personas match M01")
