import json
from pathlib import Path

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

###############################################################################
# M08 - FIRST METRICS
###############################################################################

def test_m08_files_exist():
    m08_files = find_all("metrics_08_first_metrics.json")
    assert len(m08_files) > 0, "[M08] ERROR: no files found"
    for file in m08_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M08] OK file={rel} found")

def test_m08_structure_and_types():
    m08_files = find_all("metrics_08_first_metrics.json")
    for file in m08_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M08] Checking structure file={rel}")
        data = load_json(file)

        assert isinstance(data, dict), f"[M08] ERROR file={rel}: root not dict"
        assert len(data) > 0, f"[M08] ERROR file={rel}: empty dict"

        required = [
            "first_blood_kills",
            "first_blood_kills_rate",
            "first_blood_assists",
            "first_blood_assists_rate",
            "first_death_count",
            "first_death_rate",
            "avg_early_takedowns",
            "avg_early_gold",
            "avg_early_damage",
            "avg_early_vision",
            "avg_early_farm",
            "match_count",
        ]

        for persona, stats in data.items():
            assert isinstance(persona, str), f"[M08] ERROR file={rel}: persona not str"
            assert isinstance(stats, dict), f"[M08] ERROR file={rel}: stats not dict"

            for key in required:
                assert key in stats, f"[M08] ERROR file={rel}: {key} missing"

            int_fields = [
                "first_blood_kills",
                "first_blood_assists",
                "first_death_count",
                "match_count",
            ]
            rate_fields = [
                "first_blood_kills_rate",
                "first_blood_assists_rate",
                "first_death_rate",
            ]
            float_fields = [
                "avg_early_takedowns",
                "avg_early_gold",
                "avg_early_damage",
                "avg_early_vision",
                "avg_early_farm",
            ]

            for key in int_fields:
                assert isinstance(stats[key], int), f"[M08] ERROR file={rel}: {key} not int"

            for key in rate_fields:
                assert isinstance(stats[key], (int, float)), f"[M08] ERROR file={rel}: {key} not number"

            for key in float_fields:
                assert isinstance(stats[key], (int, float)), f"[M08] ERROR file={rel}: {key} not number"

        print(f"[M08] OK file={rel} structure")

def test_m08_ranges_and_counts():
    m08_files = find_all("metrics_08_first_metrics.json")
    for file in m08_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M08] Checking ranges file={rel}")
        data = load_json(file)

        for persona, s in data.items():
            mc = s["match_count"]
            fbk = s["first_blood_kills"]
            fba = s["first_blood_assists"]
            fdc = s["first_death_count"]

            assert mc >= 0, f"[M08] ERROR file={rel}: match_count negative"
            assert fbk >= 0, f"[M08] ERROR file={rel}: first_blood_kills negative"
            assert fba >= 0, f"[M08] ERROR file={rel}: first_blood_assists negative"
            assert fdc >= 0, f"[M08] ERROR file={rel}: first_death_count negative"

            if mc > 0:
                assert fbk <= mc, f"[M08] ERROR file={rel}: first_blood_kills > match_count"
                assert fba <= mc, f"[M08] ERROR file={rel}: first_blood_assists > match_count"
                assert fdc <= mc, f"[M08] ERROR file={rel}: first_death_count > match_count"

            for key in [
                "first_blood_kills_rate",
                "first_blood_assists_rate",
                "first_death_rate",
            ]:
                rate = s[key]
                assert 0 <= rate <= 1, f"[M08] ERROR file={rel}: {key} out of range"

            assert 0 <= s["avg_early_takedowns"] <= 20, f"[M08] ERROR file={rel}: avg_early_takedowns invalid"
            assert 0 <= s["avg_early_gold"] <= 5000, f"[M08] ERROR file={rel}: avg_early_gold invalid"
            assert 0 <= s["avg_early_damage"] <= 50000, f"[M08] ERROR file={rel}: avg_early_damage invalid"
            assert 0 <= s["avg_early_vision"] <= 20, f"[M08] ERROR file={rel}: avg_early_vision invalid"
            assert 0 <= s["avg_early_farm"] <= 200, f"[M08] ERROR file={rel}: avg_early_farm invalid"

        print(f"[M08] OK file={rel} ranges")

def test_m08_rates_math():
    m08_files = find_all("metrics_08_first_metrics.json")
    for file in m08_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M08] Checking rate math file={rel}")
        data = load_json(file)

        for persona, s in data.items():
            mc = s["match_count"]
            fbk = s["first_blood_kills"]
            fba = s["first_blood_assists"]
            fdc = s["first_death_count"]

            rk = s["first_blood_kills_rate"]
            ra = s["first_blood_assists_rate"]
            rd = s["first_death_rate"]

            if mc == 0:
                assert rk == 0, f"[M08] ERROR file={rel}: kills_rate nonzero with match_count=0"
                assert ra == 0, f"[M08] ERROR file={rel}: assists_rate nonzero with match_count=0"
                assert rd == 0, f"[M08] ERROR file={rel}: death_rate nonzero with match_count=0"
            else:
                assert abs(rk - fbk / mc) < 1e-6, f"[M08] ERROR file={rel}: kills_rate mismatch"
                assert abs(ra - fba / mc) < 1e-6, f"[M08] ERROR file={rel}: assists_rate mismatch"
                assert abs(rd - fdc / mc) < 1e-6, f"[M08] ERROR file={rel}: death_rate mismatch"

        print(f"[M08] OK file={rel} rate math")

def test_m08_personas_match_m01():
    m08_files = find_all("metrics_08_first_metrics.json")
    m01_files = find_all("metrics_01_players_games_winrate.json")

    if len(m01_files) == 0:
        for file in m08_files:
            rel = file.relative_to(Path("data/results"))
            print(f"[M08] SKIP file={rel}: no M01 available")
        return

    m01_personas = set()
    for f in m01_files:
        d = load_json(f)
        for u in d["users"]:
            m01_personas.add(u["persona"])

    for file in m08_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M08] Checking personas vs M01 file={rel}")
        data = load_json(file)

        for persona in data.keys():
            assert persona in m01_personas, f"[M08] ERROR file={rel}: persona not in M01"

        print(f"[M08] OK file={rel} personas in M01")
