import json
from pathlib import Path

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

# ------------------------------------------------------
# Helper: detecta si un campo es metadata y no un role
# ------------------------------------------------------
def is_metadata(key):
    return key in {
        "source_L1",
        "generated_at",
        "start_date",
        "end_date"
    }

# ------------------------------------------------------
# Siempre devolvemos SOLO el bloque de roles
# ------------------------------------------------------
def extract_roles(data):
    # Caso nuevo formato → roles dentro de data["roles"]
    if "roles" in data and isinstance(data["roles"], dict):
        return data["roles"]
    # Caso antiguo → roles directamente en root
    return {k: v for k, v in data.items() if not is_metadata(k)}

# ------------------------------------------------------
# TEST 1
# ------------------------------------------------------
def test_m10_files_exist():
    m10_files = find_all("metrics_10_stats_by_rol.json")
    assert len(m10_files) > 0, "[M10] ERROR: no files found"
    for file in m10_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M10] OK file={rel} found")

# ------------------------------------------------------
# TEST 2
# ------------------------------------------------------
def test_m10_structure_and_types():
    m10_files = find_all("metrics_10_stats_by_rol.json")
    allowed_roles = {"TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY", ""}

    required_fields = [
        "games",
        "avg_damage",
        "avg_damage_taken",
        "avg_gold",
        "avg_farm",
        "avg_vision",
        "avg_turret_damage",
        "avg_kills",
        "avg_deaths",
        "avg_assists",
        "avg_kill_participation",
        "winrate",
    ]

    for file in m10_files:
        rel = file.relative_to("data/results")
        print(f"[M10] Checking structure file={rel}")

        raw = load_json(file)
        data = extract_roles(raw)

        assert isinstance(data, dict)
        assert len(data) > 0

        for role, players in data.items():
            assert isinstance(role, str)
            assert role in allowed_roles, f"[M10] ERROR: unexpected role {role}"
            assert isinstance(players, dict), f"[M10] ERROR: players for role {role} not dict"

            for persona, stats in players.items():
                assert isinstance(persona, str)
                assert isinstance(stats, dict)

                for key in required_fields:
                    assert key in stats, f"[M10] missing field {key}"

                assert isinstance(stats["games"], int)

                for key in required_fields:
                    if key != "games":
                        assert isinstance(stats[key], (int, float)), f"[M10] {key} not number"

        print(f"[M10] OK file={rel} structure")

# ------------------------------------------------------
# TEST 3
# ------------------------------------------------------
def test_m10_ranges():
    m10_files = find_all("metrics_10_stats_by_rol.json")

    for file in m10_files:
        rel = file.relative_to("data/results")
        print(f"[M10] Checking ranges file={rel}")

        raw = load_json(file)
        data = extract_roles(raw)

        for role, players in data.items():
            for persona, s in players.items():
                assert s["games"] >= 0

                assert 0 <= s["avg_damage"] <= 1_000_000
                assert 0 <= s["avg_damage_taken"] <= 300_000
                assert 0 <= s["avg_gold"] <= 30_000
                assert 0 <= s["avg_farm"] <= 300
                assert 0 <= s["avg_vision"] <= 150
                assert 0 <= s["avg_turret_damage"] <= 50_000

                assert 0 <= s["avg_kills"] <= 30
                assert 0 <= s["avg_deaths"] <= 30
                assert 0 <= s["avg_assists"] <= 40

                assert 0 <= s["avg_kill_participation"] <= 100
                assert 0 <= s["winrate"] <= 100

        print(f"[M10] OK file={rel} ranges")

# ------------------------------------------------------
# TEST 4
# ------------------------------------------------------
def test_m10_games_sum_match_m01():
    m10_files = find_all("metrics_10_stats_by_rol.json")

    for file in m10_files:
        rel = file.relative_to("data/results")
        print(f"[M10] Checking games vs M01 file={rel}")

        raw_m10 = load_json(file)
        data_m10 = extract_roles(raw_m10)

        pool_dir = file.parents[2]
        m01_path = pool_dir / file.parent.name / "metrics_01_players_games_winrate.json"

        if not m01_path.exists():
            print(f"[M10] SKIP file={rel}: M01 not found")
            continue

        data_m01 = load_json(m01_path)
        users = data_m01.get("users", [])

        m01_games = {
            u["persona"]: u["total_games"]
            for u in users
            if isinstance(u.get("persona"), str)
        }

        m10_games = {}
        for role, players in data_m10.items():
            for persona, stats in players.items():
                m10_games.setdefault(persona, 0)
                m10_games[persona] += stats["games"]

        for persona, games_sum in m10_games.items():
            if persona not in m01_games:
                continue
            assert games_sum == m01_games[persona], (
                f"[M10] ERROR: games mismatch for {persona} "
                f"(roles={games_sum}, M01={m01_games[persona]})"
            )

        print(f"[M10] OK file={rel} games match M01")
