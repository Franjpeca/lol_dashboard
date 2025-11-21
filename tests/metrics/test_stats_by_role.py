import json
from pathlib import Path

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def test_m10_files_exist():
    m10_files = find_all("metrics_10_stats_by_rol.json")
    assert len(m10_files) > 0, "[M10] ERROR: no files found"
    for file in m10_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M10] OK file={rel} found")

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
        rel = file.relative_to(Path("data/results"))
        print(f"[M10] Checking structure file={rel}")
        data = load_json(file)

        assert isinstance(data, dict), f"[M10] ERROR file={rel}: root not dict"
        assert len(data) > 0, f"[M10] ERROR file={rel}: empty dict"

        for role, players in data.items():
            assert isinstance(role, str), f"[M10] ERROR file={rel}: role key not str"
            assert role in allowed_roles, f"[M10] ERROR file={rel}: unexpected role {role}"
            assert isinstance(players, dict), f"[M10] ERROR file={rel}: players for role {role} not dict"

            for persona, stats in players.items():
                assert isinstance(persona, str), f"[M10] ERROR file={rel}: persona not str"
                assert isinstance(stats, dict), f"[M10] ERROR file={rel}: stats not dict"

                for key in required_fields:
                    assert key in stats, f"[M10] ERROR file={rel}: field {key} missing for {persona} in role {role}"

                assert isinstance(stats["games"], int), f"[M10] ERROR file={rel}: games not int"

                numeric_fields = [
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
                for key in numeric_fields:
                    assert isinstance(stats[key], (int, float)), f"[M10] ERROR file={rel}: {key} not number"

        print(f"[M10] OK file={rel} structure")

def test_m10_ranges():
    m10_files = find_all("metrics_10_stats_by_rol.json")
    for file in m10_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M10] Checking ranges file={rel}")
        data = load_json(file)

        for role, players in data.items():
            for persona, s in players.items():
                g = s["games"]
                assert g >= 0, f"[M10] ERROR file={rel}: games negative for {persona} in role {role}"

                assert 0 <= s["avg_damage"] <= 1000000, f"[M10] ERROR file={rel}: avg_damage invalid for {persona} in role {role}"
                assert 0 <= s["avg_damage_taken"] <= 300000, f"[M10] ERROR file={rel}: avg_damage_taken invalid for {persona} in role {role}"
                assert 0 <= s["avg_gold"] <= 30000, f"[M10] ERROR file={rel}: avg_gold invalid for {persona} in role {role}"
                assert 0 <= s["avg_farm"] <= 300, f"[M10] ERROR file={rel}: avg_farm invalid for {persona} in role {role}"
                assert 0 <= s["avg_vision"] <= 150, f"[M10] ERROR file={rel}: avg_vision invalid for {persona} in role {role}"
                assert 0 <= s["avg_turret_damage"] <= 50000, f"[M10] ERROR file={rel}: avg_turret_damage invalid for {persona} in role {role}"

                assert 0 <= s["avg_kills"] <= 30, f"[M10] ERROR file={rel}: avg_kills invalid for {persona} in role {role}"
                assert 0 <= s["avg_deaths"] <= 30, f"[M10] ERROR file={rel}: avg_deaths invalid for {persona} in role {role}"
                assert 0 <= s["avg_assists"] <= 40, f"[M10] ERROR file={rel}: avg_assists invalid for {persona} in role {role}"

                assert 0 <= s["avg_kill_participation"] <= 100, f"[M10] ERROR file={rel}: avg_kill_participation invalid for {persona} in role {role}"
                assert 0 <= s["winrate"] <= 100, f"[M10] ERROR file={rel}: winrate invalid for {persona} in role {role}"

        print(f"[M10] OK file={rel} ranges")

def test_m10_games_sum_match_m01():
    m10_files = find_all("metrics_10_stats_by_rol.json")

    for file in m10_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M10] Checking games vs M01 file={rel}")

        data_m10 = load_json(file)

        pool_dir = file.parents[2]
        m01_path = pool_dir / file.parent.name / "metrics_01_players_games_winrate.json"

        if not m01_path.exists():
            print(f"[M10] SKIP file={rel}: no M01 in same pool/min")
            continue

        data_m01 = load_json(m01_path)
        users = data_m01.get("users", [])
        if not users:
            print(f"[M10] SKIP file={rel}: M01 has no users")
            continue

        m01_games = {u["persona"]: u["total_games"]
                     for u in users
                     if isinstance(u.get("persona"), str) and isinstance(u.get("total_games"), int)}

        if not m01_games:
            print(f"[M10] SKIP file={rel}: no valid personas in M01")
            continue

        m10_games = {}
        for role, players in data_m10.items():
            for persona, stats in players.items():
                m10_games.setdefault(persona, 0)
                m10_games[persona] += stats["games"]

        for persona, total_by_roles in m10_games.items():
            if persona not in m01_games:
                print(f"[M10] SKIP persona {persona}: not present in M01")
                continue

            total_m01 = m01_games[persona]
            assert total_by_roles == total_m01, (
                f"[M10] ERROR file={rel}: games mismatch for {persona}: "
                f"sum by roles={total_by_roles}, M01={total_m01}"
            )

        print(f"[M10] OK file={rel} games match M01")