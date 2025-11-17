import os
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")
COLL_L0 = os.getenv("MONGO_COLLECTION_RAW_MATCHES", "L0_all_raw_matches")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
coll = db[COLL_L0]


def extract_match_name(p):
    """
    Devuelve SIEMPRE el nombre EXACTO con el que jugó en esa partida.
    No depende de ningún dato externo fuera del match.
    """

    # Nombre que aparece en la partida (MUY CONFIABLE)
    name = p.get("summonerName")
    if name:
        return name

    # Riot ID en la partida (Nuevo sistema)
    game = p.get("riotIdGameName")
    tag = p.get("riotIdTagline")

    if game and tag:
        return f"{game}#{tag}"

    if game:
        return game

    return "(Sin nombre)"


def load_match_summary(match_id: str, allowed_queues=None, min_amigos: int = 1):
    doc = coll.find_one({"_id": match_id})
    if not doc:
        return {"error": f"No existe la partida {match_id} en L0"}

    data = doc.get("data", {})
    info = data.get("info")
    metadata = data.get("metadata")

    if not info or not metadata:
        return {"error": "Partida corrupta o incompleta en L0"}

    queue_id = info.get("queueId")

    if allowed_queues is not None and queue_id not in allowed_queues:
        return {"error": f"Partida con cola {queue_id}, fuera de {allowed_queues}"}

    participants_puuids = metadata.get("participants", [])
    if len(participants_puuids) < min_amigos:
        return {"error": f"Partida {match_id} no cumple min_amigos={min_amigos}"}

    blue_team = []
    red_team = []

    for p in info.get("participants", []):
        farm = p.get("totalMinionsKilled", 0) + p.get("neutralMinionsKilled", 0)

        entry = {
            "puuid": p.get("puuid"),
            "name": extract_match_name(p),      # <--- AQUI EL CAMBIO IMPORTANTE
            "champ": p.get("championName"),
            "kills": p.get("kills", 0),
            "deaths": p.get("deaths", 0),
            "assists": p.get("assists", 0),
            "kda": round((p.get("kills", 0) + p.get("assists", 0)) / max(1, p.get("deaths", 0)), 2),
            "farm": farm,
            "role": p.get("teamPosition"),
            "win": p.get("win", False),
        }

        if p.get("teamId") == 100:
            blue_team.append(entry)
        else:
            red_team.append(entry)

    teams_raw = info.get("teams", [])
    blue_win = next((t["win"] for t in teams_raw if t["teamId"] == 100), False)
    red_win = next((t["win"] for t in teams_raw if t["teamId"] == 200), False)

    duration_seconds = info.get("gameDuration", 0)
    timestamp = info.get("gameStartTimestamp")

    if timestamp:
        start_time = datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")
    else:
        start_time = None

    result = {
        "matchId": match_id,
        "queueId": queue_id,
        "duration": duration_seconds,
        "start_time": start_time,
        "teams": {
            "blue": {"win": blue_win, "players": blue_team},
            "red": {"win": red_win, "players": red_team},
        }
    }

    return result


if __name__ == "__main__":
    from pprint import pprint
    test_id = "EUW1_7462388227"
    print("Probando loader...")
    result = load_match_summary(test_id)
    pprint(result)
