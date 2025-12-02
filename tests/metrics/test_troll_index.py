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

def extract_troll(data):
    assert "troll" in data, "[M07] ERROR: 'troll' key missing"
    troll = data["troll"]
    assert isinstance(troll, dict), "[M07] ERROR: 'troll' must be dict"
    return troll


def test_m07_files_exist():
    m07_files = find_all("metrics_07_troll_index.json")
    assert len(m07_files) > 0, "[M07] ERROR: no files found"


def test_m07_structure_and_types():
    m07_files = find_all("metrics_07_troll_index.json")
    for file in m07_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M07] Checking structure file={rel}")

        root = load_json(file)
        data = extract_troll(root)

        for persona, stats in data.items():
            assert isinstance(persona, str)
            assert isinstance(stats, dict)

            required = [
                "total_matches",
                "early_surrender_own", "early_surrender_enemy",
                "afk_own", "afk_enemy",
                "pct_early_surrender_own", "pct_early_surrender_enemy",
                "pct_afk_own", "pct_afk_enemy"
            ]

            for key in required:
                assert key in stats, f"[M07] ERROR file={rel}: {key} missing"

            assert isinstance(stats["total_matches"], int)
            assert isinstance(stats["early_surrender_own"], int)
            assert isinstance(stats["early_surrender_enemy"], int)
            assert isinstance(stats["afk_own"], int)
            assert isinstance(stats["afk_enemy"], int)

            for key in [
                "pct_early_surrender_own", "pct_early_surrender_enemy",
                "pct_afk_own", "pct_afk_enemy"
            ]:
                assert isinstance(stats[key], (int, float))

        print(f"[M07] OK file={rel} structure")


def test_m07_ranges_and_limits():
    m07_files = find_all("metrics_07_troll_index.json")

    for file in m07_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M07] Checking ranges file={rel}")

        root = load_json(file)
        data = extract_troll(root)

        for persona, s in data.items():

            tm = s["total_matches"]
            assert tm >= 0

            for key in ["early_surrender_own", "early_surrender_enemy", "afk_own", "afk_enemy"]:
                assert 0 <= s[key] <= tm

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

        root = load_json(file)
        data = extract_troll(root)

        for persona, s in data.items():
            tm = s["total_matches"]
            eo = s["early_surrender_own"]
            ee = s["early_surrender_enemy"]
            ao = s["afk_own"]
            ae = s["afk_enemy"]

            if tm == 0:
                assert s["pct_early_surrender_own"] == 0
                assert s["pct_early_surrender_enemy"] == 0
                assert s["pct_afk_own"] == 0
                assert s["pct_afk_enemy"] == 0
            else:
                assert abs(s["pct_early_surrender_own"] - eo / tm) < 1e-6
                assert abs(s["pct_early_surrender_enemy"] - ee / tm) < 1e-6
                assert abs(s["pct_afk_own"] - ao / tm) < 1e-6
                assert abs(s["pct_afk_enemy"] - ae / tm) < 1e-6

        print(f"[M07] OK file={rel} percentage math")


def test_m07_personas_match_m01():
    m07_files = find_all("metrics_07_troll_index.json")
    m01_files = find_all("metrics_01_players_games_winrate.json")

    if len(m01_files) == 0:
        return

    m01_personas = set()
    for f in m01_files:
        d = load_json(f)
        for u in d["users"]:
            m01_personas.add(u["persona"])

    for file in m07_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M07] Checking personas vs M01 file={rel}")

        root = load_json(file)
        data = extract_troll(root)

        for persona in data.keys():
            assert persona in m01_personas, f"[M07] ERROR file={rel}: persona not in M01"

        print(f"[M07] OK file={rel} personas match M01")
