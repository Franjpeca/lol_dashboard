import os
import json
import datetime
from collections import defaultdict
from pathlib import Path

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

BASE_DIR = Path(__file__).resolve().parents[2]
RESULTS_DIR = BASE_DIR / "data" / "results"
RESULTS_DIR.mkdir(parents=True, exist_ok=True)

# Valores por defecto
DEFAULT_QUEUE = 440  # Flex
DEFAULT_MIN = 5  # Mínimo de amigos
DEFAULT_POOL = "ac89fa8d"  # Pool por defecto

def to_date(ts_ms):
    """Convierte timestamp en milisegundos de Riot a YYYY-MM-DD."""
    if not ts_ms:
        return None
    dt = datetime.datetime.utcfromtimestamp(ts_ms / 1000)
    return dt.date().isoformat()


def ensure_date_range(daily_dict, min_date, max_date):
    """Rellena días faltantes en [min_date, max_date] con games = 0."""
    start = datetime.date.fromisoformat(min_date)
    end = datetime.date.fromisoformat(max_date)

    out = []
    cur = start
    while cur <= end:
        ds = cur.isoformat()
        out.append({"date": ds, "games": daily_dict.get(ds, 0)})
        cur += datetime.timedelta(days=1)
    return out


def save_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def load_users_index():
    """
    Lee L0_users_index y construye:
      - users_by_persona: persona -> {riotIds, puuids}
      - puuid_to_persona: puuid -> persona
      - personas_index: lista de personas
    """
    coll = db["L0_users_index"]

    users_by_persona = {}
    puuid_to_persona = {}
    personas_index = []

    cursor = coll.find({}, {"_id": 0, "persona": 1, "riotIds": 1, "puuids": 1})

    for doc in cursor:
        persona = doc.get("persona")
        if not persona:
            continue

        personas_index.append(persona)
        riot_ids = doc.get("riotIds", [])
        puuids = doc.get("puuids", [])

        users_by_persona[persona] = {
            "riotIds": riot_ids,
            "puuids": puuids,
        }

        for pid in puuids:
            puuid_to_persona[pid] = persona

    return users_by_persona, puuid_to_persona, personas_index


def main():
    print("[03] Starting ... using collection: L1_q440_min5_pool_ac89fa8d")

    users_by_persona, puuid_to_persona, personas_index = load_users_index()
    if not users_by_persona:
        pass

    # Buscar colecciones L1 filtradas
    l1_collections = [
        name for name in db.list_collection_names()
        if name.startswith("L1_q") and f"min{DEFAULT_MIN}" in name and f"pool_{DEFAULT_POOL}" in name
    ]

    if not l1_collections:
        return

    global_days = defaultdict(int)                       # date -> games
    player_days = defaultdict(lambda: defaultdict(int))  # persona -> date -> games

    # Inicializar personas conocidas
    for persona in personas_index:
        _ = player_days[persona]

    min_date = None
    max_date = None

    for coll_name in l1_collections:
        coll = db[coll_name]

        cursor = coll.find({}, {"data.info": 1, "friends_present": 1})

        for doc in cursor:
            info = doc.get("data", {}).get("info", {}) or {}

            date = (
                to_date(info.get("gameEndTimestamp"))
                or to_date(info.get("gameStartTimestamp"))
                or to_date(info.get("gameCreation"))
            )
            if date is None:
                continue

            if min_date is None or date < min_date:
                min_date = date
            if max_date is None or date > max_date:
                max_date = date

            # Global: una partida cuenta una vez
            global_days[date] += 1

            # Personas presentes en la partida según puuids amigos
            friends_present = doc.get("friends_present", []) or []
            personas_en_partida = set()

            for pid in friends_present:
                persona = puuid_to_persona.get(pid)
                if persona:
                    personas_en_partida.add(persona)

            for persona in personas_en_partida:
                player_days[persona][date] += 1

    if min_date is None:
        return

    # Extender hasta hoy, aunque no haya partidas recientes
    today_str = datetime.date.today().isoformat()
    if today_str > max_date:
        max_date = today_str

    # Serie global rellenada
    global_series = ensure_date_range(global_days, min_date, max_date)

    # Serie por persona rellenada
    players_out = []
    # Incluir también personas que hayan aparecido aunque no estuvieran en L0
    all_personas = set(personas_index) | set(player_days.keys())

    for persona in sorted(all_personas):
        by_day = player_days.get(persona, {})
        filled = ensure_date_range(by_day, min_date, max_date)
        total = sum(x["games"] for x in filled)

        user_info = users_by_persona.get(persona, {})
        riot_ids = user_info.get("riotIds", [])
        puuids = user_info.get("puuids", [])

        players_out.append(
            {
                "persona": persona,
                "riotIds": riot_ids,
                "puuids": puuids,
                "total_games": total,
                "games": filled,
            }
        )

    # Guardar todo junto en un solo archivo JSON
    save_json(
        RESULTS_DIR / "pool_ac89fa8d" / "q440" / "min5" / "metrics_03_games_frecuency.json",
        {
            "global_frequency": global_series,
            "players_frequency": players_out,
        }
    )

    print("[03] Ended")


if __name__ == "__main__":
    main()
