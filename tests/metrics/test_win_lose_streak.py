import json
from pathlib import Path

def find_all(pattern):
    root = Path("data/results")
    return list(root.rglob(pattern))

def load_json(path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

###############################################################################
# M04 - WIN / LOSE STREAK
###############################################################################

def extract_streaks(data):
    # Nuevo formato: streaks dentro de "streaks"
    assert "streaks" in data, "[M04] ERROR: 'streaks' key missing"
    streaks = data["streaks"]
    assert isinstance(streaks, dict), "[M04] ERROR: 'streaks' must be a dict"
    return streaks


def test_m04_files_exist():
    m04_files = find_all("metrics_04_win_lose_streak.json")
    assert len(m04_files) > 0, "[M04] ERROR: no files found"


def test_m04_structure_and_types():
    m04_files = find_all("metrics_04_win_lose_streak.json")

    for file in m04_files:
        data = load_json(file)
        streaks = extract_streaks(data)

        for persona, st in streaks.items():
            assert isinstance(persona, str)
            assert isinstance(st, dict)

            assert "max_win_streak" in st
            assert "max_lose_streak" in st
            assert "current_streak" in st

            assert isinstance(st["max_win_streak"], int)
            assert isinstance(st["max_lose_streak"], int)
            assert isinstance(st["current_streak"], int)


def test_m04_ranges():
    m04_files = find_all("metrics_04_win_lose_streak.json")

    for file in m04_files:
        data = load_json(file)
        streaks = extract_streaks(data)

        for persona, st in streaks.items():
            mws = st["max_win_streak"]
            mls = st["max_lose_streak"]
            cs = st["current_streak"]

            assert mws >= 0
            assert mls >= 0
            assert abs(cs) <= 1000
            assert mws <= 1000
            assert mls <= 1000


def test_m04_internal_consistency():
    m04_files = find_all("metrics_04_win_lose_streak.json")

    for file in m04_files:
        data = load_json(file)
        streaks = extract_streaks(data)

        for persona, st in streaks.items():
            mws = st["max_win_streak"]
            mls = st["max_lose_streak"]
            cs = st["current_streak"]

            if cs > 0:
                assert cs <= mws
            if cs < 0:
                assert abs(cs) <= mls

            if mws == 0:
                assert cs <= 0
            if mls == 0:
                assert cs >= 0


def test_m04_persona_name_validity_against_m01():

    m04_files = find_all("metrics_04_win_lose_streak.json")
    m01_files = find_all("metrics_01_players_games_winrate.json")

    if len(m01_files) == 0:
        return

    # Personas desde M01
    m01_personas = set()
    for f in m01_files:
        d = load_json(f)
        for u in d["users"]:
            m01_personas.add(u["persona"])

    # Validar que todas las personas de streaks están en M01
    for file in m04_files:
        data = load_json(file)
        streaks = extract_streaks(data)

        for persona in streaks.keys():
            assert persona in m01_personas, f"persona not found in M01"
