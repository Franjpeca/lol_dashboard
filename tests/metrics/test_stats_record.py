import json
import re
from pathlib import Path

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

# ------------------------------------------------------
# Campos que NO son personas y deben ignorarse
# ------------------------------------------------------
METADATA_KEYS = {
    "source_L1",
    "generated_at",
    "start_date",
    "end_date"
}

def extract_personas_block(data):
    """
    Devuelve solo el bloque con las personas.
    Compatible con:
    - Formato nuevo (con metadatos)
    - Formato antiguo
    """
    # Caso nuevo: bloque interno "records"
    if "records" in data and isinstance(data["records"], dict):
        return data["records"]

    # Caso antiguo: root contiene solo personas
    return {k: v for k, v in data.items() if k not in METADATA_KEYS}


###############################################################################
# TESTS
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

        raw = load_json(file)
        data = extract_personas_block(raw)

        assert isinstance(data, dict), f"[M11] ERROR file={rel}: root not dict"

        for persona, stats in data.items():
            assert isinstance(persona, str)
            assert isinstance(stats, dict)

            for key in required:
                assert key in stats, f"[M11] Missing {key} for {persona}"

                entry = stats[key]
                assert isinstance(entry, dict)

                assert "value" in entry
                assert "game_id" in entry

                if key == "longest_game":
                    assert isinstance(entry["value"], str)
                    assert re.match(r"^\d+m \d+s$", entry["value"])
                else:
                    assert isinstance(entry["value"], int)

                assert isinstance(entry["game_id"], int)

        print(f"[M11] OK file={rel} structure")


def test_m11_ranges():
    m11_files = find_all("metrics_11_stats_record.json")

    for file in m11_files:
        rel = file.relative_to("data/results")
        print(f"[M11] Checking ranges file={rel}")

        raw = load_json(file)
        data = extract_personas_block(raw)

        for persona, stats in data.items():
            assert 0 <= stats["max_kills"]["value"] <= 50
            assert 0 <= stats["max_deaths"]["value"] <= 50
            assert 0 <= stats["max_assists"]["value"] <= 60
            assert 0 <= stats["max_vision_score"]["value"] <= 200
            assert 0 <= stats["max_farm"]["value"] <= 500
            assert 0 <= stats["max_damage_dealt"]["value"] <= 200000
            assert 0 <= stats["max_gold"]["value"] <= 40000

            lg = stats["longest_game"]["value"]
            assert re.match(r"^\d+m \d+s$", lg)

            gid = stats["max_kills"]["game_id"]
            assert gid > 0

        print(f"[M11] OK file={rel} ranges")


def test_m11_personas_match_m01():
    m11_files = find_all("metrics_11_stats_record.json")

    for file in m11_files:
        rel = file.relative_to("data/results")
        print(f"[M11] Checking personas vs M01 file={rel}")

        folder = file.parent
        m01_path = folder / "metrics_01_players_games_winrate.json"

        if not m01_path.exists():
            print(f"[M11] SKIP file={rel}: no M01 found")
            continue

        data_m01 = load_json(m01_path)
        users = data_m01.get("users", [])

        m01_personas = {
            u["persona"]
            for u in users
            if isinstance(u.get("persona"), str)
        }

        raw = load_json(file)
        data_m11 = extract_personas_block(raw)

        for persona in data_m11.keys():
            assert persona in m01_personas, (
                f"[M11] ERROR file={rel}: persona {persona} not in M01"
            )

        print(f"[M11] OK file={rel} personas match M01")
