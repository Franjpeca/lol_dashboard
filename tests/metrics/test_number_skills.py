import json
from pathlib import Path


def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))


def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def test_m09_files_exist():
    m09_files = find_all("metrics_09_number_skills.json")
    assert len(m09_files) > 0, "[M09] ERROR: no files found"
    for file in m09_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M09] OK file={rel} found")


def test_m09_structure_and_types():
    m09_files = find_all("metrics_09_number_skills.json")
    for file in m09_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M09] Checking structure file={rel}")
        data = load_json(file)

        assert isinstance(data, dict), f"[M09] ERROR file={rel}: root not dict"
        assert "skills" in data, f"[M09] ERROR file={rel}: 'skills' key missing"

        skills = data["skills"]
        assert isinstance(skills, dict), f"[M09] ERROR file={rel}: skills not dict"
        assert len(skills) > 0, f"[M09] ERROR file={rel}: skills empty"

        required = [
            "avg_Q", "avg_W", "avg_E", "avg_R",
            "max_Q", "max_W", "max_E", "max_R",
            "matches",
        ]

        for persona, stats in skills.items():
            assert isinstance(persona, str), f"[M09] ERROR file={rel}: persona not str"
            assert isinstance(stats, dict), f"[M09] ERROR file={rel}: stats not dict"

            for key in required:
                assert key in stats, f"[M09] ERROR file={rel}: {key} missing for {persona}"

            for key in ["avg_Q", "avg_W", "avg_E", "avg_R"]:
                assert isinstance(
                    stats[key], (int, float)
                ), f"[M09] ERROR file={rel}: {key} not number for {persona}"

            for key in ["max_Q", "max_W", "max_E", "max_R"]:
                assert isinstance(
                    stats[key], int
                ), f"[M09] ERROR file={rel}: {key} not int for {persona}"

            assert isinstance(
                stats["matches"], int
            ), f"[M09] ERROR file={rel}: matches not int for {persona}"

        print(f"[M09] OK file={rel} structure")


def test_m09_ranges():
    m09_files = find_all("metrics_09_number_skills.json")
    for file in m09_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M09] Checking ranges file={rel}")
        data = load_json(file)

        skills = data.get("skills", {})
        for persona, s in skills.items():
            assert s["matches"] >= 0, f"[M09] ERROR file={rel}: matches negative for {persona}"

            assert 0 <= s["avg_Q"] <= 400, f"[M09] ERROR file={rel}: avg_Q invalid for {persona}"
            assert 0 <= s["avg_W"] <= 200, f"[M09] ERROR file={rel}: avg_W invalid for {persona}"
            assert 0 <= s["avg_E"] <= 300, f"[M09] ERROR file={rel}: avg_E invalid for {persona}"
            assert 0 <= s["avg_R"] <= 50, f"[M09] ERROR file={rel}: avg_R invalid for {persona}"

            assert 0 <= s["max_Q"] <= 2000, f"[M09] ERROR file={rel}: max_Q invalid for {persona}"
            assert 0 <= s["max_W"] <= 1000, f"[M09] ERROR file={rel}: max_W invalid for {persona}"
            assert 0 <= s["max_E"] <= 1000, f"[M09] ERROR file={rel}: max_E invalid for {persona}"
            assert 0 <= s["max_R"] <= 1000, f"[M09] ERROR file={rel}: max_R invalid for {persona}"

        print(f"[M09] OK file={rel} ranges")


def test_m09_coherence_max_greater_equal_avg():
    m09_files = find_all("metrics_09_number_skills.json")
    for file in m09_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M09] Checking max >= avg file={rel}")
        data = load_json(file)

        skills = data.get("skills", {})
        for persona, s in skills.items():
            assert s["max_Q"] >= s["avg_Q"], f"[M09] ERROR file={rel}: max_Q < avg_Q for {persona}"
            assert s["max_W"] >= s["avg_W"], f"[M09] ERROR file={rel}: max_W < avg_W for {persona}"
            assert s["max_E"] >= s["avg_E"], f"[M09] ERROR file={rel}: max_E < avg_E for {persona}"
            assert s["max_R"] >= s["avg_R"], f"[M09] ERROR file={rel}: max_R < avg_R for {persona}"

        print(f"[M09] OK file={rel} max >= avg")


def test_m09_matches_consistency():
    m09_files = find_all("metrics_09_number_skills.json")
    for file in m09_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M09] Checking matches consistency file={rel}")
        data = load_json(file)

        skills = data.get("skills", {})
        for persona, s in skills.items():
            if s["matches"] == 0:
                assert s["avg_Q"] == 0, f"[M09] ERROR file={rel}: avg_Q nonzero with matches=0 for {persona}"
                assert s["avg_W"] == 0, f"[M09] ERROR file={rel}: avg_W nonzero with matches=0 for {persona}"
                assert s["avg_E"] == 0, f"[M09] ERROR file={rel}: avg_E nonzero with matches=0 for {persona}"
                assert s["avg_R"] == 0, f"[M09] ERROR file={rel}: avg_R nonzero with matches=0 for {persona}"
                assert s["max_Q"] == 0, f"[M09] ERROR file={rel}: max_Q nonzero with matches=0 for {persona}"
                assert s["max_W"] == 0, f"[M09] ERROR file={rel}: max_W nonzero with matches=0 for {persona}"
                assert s["max_E"] == 0, f"[M09] ERROR file={rel}: max_E nonzero with matches=0 for {persona}"
                assert s["max_R"] == 0, f"[M09] ERROR file={rel}: max_R nonzero with matches=0 for {persona}"

        print(f"[M09] OK file={rel} matches consistency")


def test_m09_personas_match_m01():
    m09_files = find_all("metrics_09_number_skills.json")
    m01_files = find_all("metrics_01_players_games_winrate.json")

    if len(m01_files) == 0:
        for file in m09_files:
            rel = file.relative_to(Path("data/results"))
            print(f"[M09] SKIP file={rel}: no M01 available")
        return

    m01_personas = set()
    for f in m01_files:
        d = load_json(f)
        for u in d["users"]:
            m01_personas.add(u["persona"])

    for file in m09_files:
        rel = file.relative_to(Path("data/results"))
        print(f"[M09] Checking personas vs M01 file={rel}")
        data = load_json(file)

        skills = data.get("skills", {})
        for persona in skills.keys():
            assert persona in m01_personas, f"[M09] ERROR file={rel}: persona {persona} not in M01"

        print(f"[M09] OK file={rel} personas match M01")
