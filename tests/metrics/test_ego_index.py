import json
from pathlib import Path

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


###############################################################################
# M06 - EGO INDEX
###############################################################################


def extract_ego(data):
    assert "ego" in data, "[M06] ERROR: 'ego' key missing"
    ego = data["ego"]
    assert isinstance(ego, dict), "[M06] ERROR: 'ego' must be dict"
    return ego


def test_m06_files_exist():
    m06_files = find_all("metrics_06_ego_index.json")
    assert len(m06_files) > 0, "[M06] ERROR: no files found"
    for file in m06_files:
        print(f"[M06] OK file={file} found")


def test_m06_structure_and_types():
    m06_files = find_all("metrics_06_ego_index.json")
    for file in m06_files:
        data = load_json(file)
        ego = extract_ego(data)

        for persona, stats in ego.items():
            assert isinstance(persona, str), f"[M06] persona not str"
            assert isinstance(stats, dict), f"[M06] stats not dict"

            required = [
                "ego_index", "selfish_score", "teamplay_score", "tilt_score",
                "match_count", "avg_kills", "avg_deaths", "avg_assists",
                "avg_gold", "avg_damage_dealt", "avg_vision_score",
                "lost_by_surrender", "lost_surrender_rate"
            ]

            for key in required:
                assert key in stats, f"[M06] missing {key}"

            for key in [
                "ego_index", "selfish_score", "teamplay_score", "tilt_score",
                "avg_kills", "avg_deaths", "avg_assists", "avg_gold",
                "avg_damage_dealt", "avg_vision_score",
                "lost_surrender_rate"
            ]:
                assert isinstance(stats[key], (int, float)), f"[M06] {key} not number"

            assert isinstance(stats["match_count"], int)
            assert isinstance(stats["lost_by_surrender"], int)


def test_m06_ranges():
    m06_files = find_all("metrics_06_ego_index.json")
    for file in m06_files:
        data = load_json(file)
        ego = extract_ego(data)

        for persona, s in ego.items():
            assert -5 <= s["ego_index"] <= 5

            for key in ["selfish_score", "teamplay_score", "tilt_score"]:
                assert 0 <= s[key] <= 5

            assert 0 <= s["avg_kills"] <= 40
            assert 0 <= s["avg_deaths"] <= 40
            assert 0 <= s["avg_assists"] <= 50
            assert 0 <= s["avg_gold"] <= 30000
            assert 0 <= s["avg_damage_dealt"] <= 200000
            assert 0 <= s["avg_vision_score"] <= 200

            assert s["match_count"] >= 0
            assert s["lost_by_surrender"] >= 0
            assert s["lost_by_surrender"] <= s["match_count"]


def test_m06_surrender_rate_math():
    m06_files = find_all("metrics_06_ego_index.json")
    for file in m06_files:
        data = load_json(file)
        ego = extract_ego(data)

        for persona, s in ego.items():
            mc = s["match_count"]
            ls = s["lost_by_surrender"]
            rate = s["lost_surrender_rate"]

            if mc == 0:
                assert rate == 0
            else:
                assert abs(rate - (ls / mc)) < 1e-6


def test_m06_personas_match_m01():
    m06_files = find_all("metrics_06_ego_index.json")
    m01_files = find_all("metrics_01_players_games_winrate.json")

    if len(m01_files) == 0:
        return

    m01_personas = set()
    for f in m01_files:
        d = load_json(f)
        for u in d["users"]:
            m01_personas.add(u["persona"])

    for file in m06_files:
        data = load_json(file)
        ego = extract_ego(data)

        for persona in ego.keys():
            assert persona in m01_personas, f"[M06] persona '{persona}' not in M01"
