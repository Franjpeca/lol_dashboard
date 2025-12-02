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

def extract_metrics(data):
    assert "first_metrics" in data, "[M08] ERROR: missing first_metrics key"
    players = data["first_metrics"]
    assert isinstance(players, dict), "[M08] ERROR: first_metrics must be dict"
    return players


def test_m08_files_exist():
    m08_files = find_all("metrics_08_first_metrics.json")
    assert len(m08_files) > 0, "[M08] ERROR: no files found"


def test_m08_structure_and_types():
    m08_files = find_all("metrics_08_first_metrics.json")
    for file in m08_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M08] Checking structure file={rel}")

        data = load_json(file)
        stats_data = extract_metrics(data)

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

        for persona, stats in stats_data.items():
            assert isinstance(persona, str), f"[M08] ERROR file={rel}: persona not str"
            assert isinstance(stats, dict), f"[M08] ERROR file={rel}: stats not dict"

            for key in required:
                assert key in stats, f"[M08] ERROR file={rel}: {key} missing"

        print(f"[M08] OK file={rel} structure")


def test_m08_ranges_and_counts():
    m08_files = find_all("metrics_08_first_metrics.json")
    for file in m08_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M08] Checking ranges file={rel}")

        data = load_json(file)
        stats_data = extract_metrics(data)

        for persona, s in stats_data.items():
            mc = s["match_count"]

            assert mc >= 0
            assert s["first_blood_kills"] >= 0
            assert s["first_blood_assists"] >= 0
            assert s["first_death_count"] >= 0

            if mc > 0:
                assert s["first_blood_kills"] <= mc
                assert s["first_blood_assists"] <= mc
                assert s["first_death_count"] <= mc

            assert 0 <= s["first_blood_kills_rate"] <= 1
            assert 0 <= s["first_blood_assists_rate"] <= 1
            assert 0 <= s["first_death_rate"] <= 1

        print(f"[M08] OK file={rel} ranges")


def test_m08_rates_math():
    m08_files = find_all("metrics_08_first_metrics.json")
    for file in m08_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M08] Checking rate math file={rel}")

        data = load_json(file)
        stats_data = extract_metrics(data)

        for persona, s in stats_data.items():
            mc = s["match_count"]
            fbk = s["first_blood_kills"]
            fba = s["first_blood_assists"]
            fdc = s["first_death_count"]

            if mc == 0:
                assert s["first_blood_kills_rate"] == 0
                assert s["first_blood_assists_rate"] == 0
                assert s["first_death_rate"] == 0
            else:
                assert abs(s["first_blood_kills_rate"] - fbk / mc) < 1e-6
                assert abs(s["first_blood_assists_rate"] - fba / mc) < 1e-6
                assert abs(s["first_death_rate"] - fdc / mc) < 1e-6

        print(f"[M08] OK file={rel} rate math")


def test_m08_personas_match_m01():
    m08_files = find_all("metrics_08_first_metrics.json")
    m01_files = find_all("metrics_01_players_games_winrate.json")

    if len(m01_files) == 0:
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
        stats_data = extract_metrics(data)

        for persona in stats_data.keys():
            assert persona in m01_personas, f"[M08] ERROR file={rel}: persona not in M01"

        print(f"[M08] OK file={rel} personas in M01")
