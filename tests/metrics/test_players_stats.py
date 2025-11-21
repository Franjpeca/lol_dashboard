import json
from pathlib import Path

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

###############################################################################
# M05 - PLAYERS STATS
###############################################################################

def test_m05_files_exist():
    m05_files = find_all("metrics_05_players_stats.json")
    assert len(m05_files) > 0, "[M05] ERROR: no files found"
    for file in m05_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M05] OK file={rel} found")

def test_m05_structure():
    m05_files = find_all("metrics_05_players_stats.json")
    for file in m05_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M05] Checking structure file={rel}")
        data = load_json(file)

        assert isinstance(data, dict), f"[M05] ERROR file={rel}: root not dict"
        assert len(data) > 0, f"[M05] ERROR file={rel}: empty dict"

        for persona, stats in data.items():
            assert isinstance(persona, str), f"[M05] ERROR file={rel}: persona not str"
            assert isinstance(stats, dict), f"[M05] ERROR file={rel}: stats not dict"

            for key in [
                "avg_kda", "avg_kills", "avg_deaths", "avg_assists",
                "avg_gold", "avg_damage_dealt", "avg_damage_taken", "avg_vision_score",
                "max_kills", "max_deaths", "max_assists"
            ]:
                assert key in stats, f"[M05] ERROR file={rel}: {key} missing"

        print(f"[M05] OK file={rel} structure")

def test_m05_value_types():
    m05_files = find_all("metrics_05_players_stats.json")
    for file in m05_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M05] Checking types file={rel}")
        data = load_json(file)

        for persona, stats in data.items():
            for key in [
                "avg_kda", "avg_kills", "avg_deaths", "avg_assists",
                "avg_gold", "avg_damage_dealt", "avg_damage_taken", "avg_vision_score"
            ]:
                assert isinstance(stats[key], (int, float)), f"[M05] ERROR file={rel}: {key} not number"

            for key in ["max_kills", "max_deaths", "max_assists"]:
                entry = stats[key]
                assert isinstance(entry, dict), f"[M05] ERROR file={rel}: {key} not dict"
                for subkey in entry.keys():
                    assert subkey in ["kills", "deaths", "assists", "match_id"], f"[M05] ERROR file={rel}: invalid field {subkey}"

        print(f"[M05] OK file={rel} types")

def test_m05_ranges():
    m05_files = find_all("metrics_05_players_stats.json")
    for file in m05_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M05] Checking ranges file={rel}")
        data = load_json(file)

        for persona, stats in data.items():
            assert stats["avg_kda"] >= 0, f"[M05] ERROR file={rel}: avg_kda negative"
            assert 0 <= stats["avg_kills"] <= 40, f"[M05] ERROR file={rel}: avg_kills invalid"
            assert 0 <= stats["avg_deaths"] <= 40, f"[M05] ERROR file={rel}: avg_deaths invalid"
            assert 0 <= stats["avg_assists"] <= 50, f"[M05] ERROR file={rel}: avg_assists invalid"
            assert 0 <= stats["avg_gold"] <= 30000, f"[M05] ERROR file={rel}: avg_gold invalid"
            assert 0 <= stats["avg_damage_dealt"] <= 200000, f"[M05] ERROR file={rel}: avg_damage_dealt invalid"
            assert 0 <= stats["avg_damage_taken"] <= 200000, f"[M05] ERROR file={rel}: avg_damage_taken invalid"
            assert 0 <= stats["avg_vision_score"] <= 200, f"[M05] ERROR file={rel}: avg_vision_score invalid"

        print(f"[M05] OK file={rel} ranges")

def test_m05_max_values_and_coherence():
    m05_files = find_all("metrics_05_players_stats.json")
    for file in m05_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M05] Checking max >= avg file={rel}")
        data = load_json(file)

        for persona, stats in data.items():
            mk = stats["max_kills"]["kills"]
            md = stats["max_deaths"]["deaths"]
            ma = stats["max_assists"]["assists"]

            assert mk >= 0, f"[M05] ERROR file={rel}: max_kills negative"
            assert md >= 0, f"[M05] ERROR file={rel}: max_deaths negative"
            assert ma >= 0, f"[M05] ERROR file={rel}: max_assists negative"

            assert mk >= stats["avg_kills"], f"[M05] ERROR file={rel}: max_kills < avg_kills"
            assert md >= stats["avg_deaths"], f"[M05] ERROR file={rel}: max_deaths < avg_deaths"
            assert ma >= stats["avg_assists"], f"[M05] ERROR file={rel}: max_assists < avg_assists"

        print(f"[M05] OK file={rel} max >= avg")

def test_m05_match_id_validity():
    m05_files = find_all("metrics_05_players_stats.json")
    for file in m05_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M05] Checking match_id file={rel}")
        data = load_json(file)

        for persona, stats in data.items():
            for key in ["max_kills", "max_deaths", "max_assists"]:
                entry = stats[key]
                assert "match_id" in entry, f"[M05] ERROR file={rel}: match_id missing"
                assert isinstance(entry["match_id"], int), f"[M05] ERROR file={rel}: match_id not int"
                assert entry["match_id"] > 0, f"[M05] ERROR file={rel}: match_id invalid"

        print(f"[M05] OK file={rel} match_id")

def test_m05_persona_match_with_m01():
    m05_files = find_all("metrics_05_players_stats.json")
    m01_files = find_all("metrics_01_players_games_winrate.json")

    if len(m01_files) == 0:
        for file in m05_files:
            rel = file.relative_to(Path("data/results"))
            print(f"[M05] SKIP file={rel}: no M01 available")
        return

    m01_personas = set()
    for f in m01_files:
        d = load_json(f)
        for u in d["users"]:
            m01_personas.add(u["persona"])

    for file in m05_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M05] Checking personas vs M01 file={rel}")
        data = load_json(file)

        for persona in data.keys():
            assert persona in m01_personas, f"[M05] ERROR file={rel}: persona not in M01"

        print(f"[M05] OK file={rel} persona names match M01")
