import json
import re
from pathlib import Path

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

###############################################################################
# M11 - STATS RECORD
###############################################################################

def test_m11_files_exist():
    m11_files = find_all("metrics_11_stats_record.json")
    assert len(m11_files) > 0, "[M11] ERROR: no files found"
    for file in m11_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M11] OK file={rel} found")

def test_m11_structure_and_types():
    m11_files = find_all("metrics_11_stats_record.json")

    required = [
        "max_kills",
        "max_deaths",
        "max_assists",
        "max_vision_score",
        "max_farm",
        "max_damage_dealt",
        "max_gold",
        "longest_game",
    ]

    for file in m11_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M11] Checking structure file={rel}")

        data = load_json(file)
        assert isinstance(data, dict), f"[M11] ERROR file={rel}: root not dict"

        for persona, stats in data.items():
            assert isinstance(persona, str), f"[M11] ERROR file={rel}: persona not str"
            assert isinstance(stats, dict), f"[M11] ERROR file={rel}: stats not dict"

            for key in required:
                assert key in stats, f"[M11] ERROR file={rel}: {key} missing for {persona}"

                entry = stats[key]
                assert isinstance(entry, dict), f"[M11] ERROR file={rel}: {key} not dict"

                assert "value" in entry, f"[M11] ERROR file={rel}: {key}.value missing"
                assert "game_id" in entry, f"[M11] ERROR file={rel}: {key}.game_id missing"

                if key == "longest_game":
                    assert isinstance(entry["value"], str), f"[M11] ERROR file={rel}: longest_game value not str"
                else:
                    assert isinstance(entry["value"], int), f"[M11] ERROR file={rel}: {key}.value not int"

                assert isinstance(entry["game_id"], int), f"[M11] ERROR file={rel}: {key}.game_id not int"

        print(f"[M11] OK file={rel} structure")

def test_m11_ranges():
    m11_files = find_all("metrics_11_stats_record.json")

    for file in m11_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M11] Checking ranges file={rel}")

        data = load_json(file)

        for persona, stats in data.items():
            assert 0 <= stats["max_kills"]["value"] <= 50, f"[M11] ERROR file={rel}: max_kills invalid"
            assert 0 <= stats["max_deaths"]["value"] <= 50, f"[M11] ERROR file={rel}: max_deaths invalid"
            assert 0 <= stats["max_assists"]["value"] <= 60, f"[M11] ERROR file={rel}: max_assists invalid"
            assert 0 <= stats["max_vision_score"]["value"] <= 200, f"[M11] ERROR file={rel}: max_vision invalid"
            assert 0 <= stats["max_farm"]["value"] <= 500, f"[M11] ERROR file={rel}: max_farm invalid"
            assert 0 <= stats["max_damage_dealt"]["value"] <= 200000, f"[M11] ERROR file={rel}: max_damage invalid"
            assert 0 <= stats["max_gold"]["value"] <= 40000, f"[M11] ERROR file={rel}: max_gold invalid"

            lg = stats["longest_game"]["value"]
            assert re.match(r"^\d+m \d+s$", lg), f"[M11] ERROR file={rel}: longest_game invalid format"

            gid = stats["max_kills"]["game_id"]
            assert gid > 0, f"[M11] ERROR file={rel}: game_id invalid"

        print(f"[M11] OK file={rel} ranges")

def test_m11_personas_match_m01():
    m11_files = find_all("metrics_11_stats_record.json")

    for file in m11_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M11] Checking personas vs M01 file={rel}")

        folder = file.parent
        m01_path = folder / "metrics_01_players_games_winrate.json"

        if not m01_path.exists():
            print(f"[M11] SKIP file={rel}: no M01 in same min-folder")
            continue

        data_m01 = load_json(m01_path)
        users = data_m01.get("users", [])
        if not users:
            print(f"[M11] SKIP file={rel}: M01 has no users")
            continue

        m01_personas = {u["persona"] for u in users if isinstance(u.get("persona"), str)}

        data_m11 = load_json(file)

        for persona in data_m11.keys():
            assert persona in m01_personas, f"[M11] ERROR file={rel}: persona {persona} not in M01"

        print(f"[M11] OK file={rel} personas in M01")
