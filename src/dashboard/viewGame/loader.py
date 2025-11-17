import os
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv

# ==========================
#   CONFIG
# ==========================

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")

COLL_L0 = os.getenv("MONGO_COLLECTION_RAW_MATCHES", "L0_all_raw_matches")
COLL_USERS = os.getenv("MONGO_COLLECTION_USERS", "L0_users_index")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

coll_matches = db[COLL_L0]
coll_users = db[COLL_USERS]


# ==============================================================
#                  CARGAR MAPA PUUID → NOMBRE
# ==============================================================

def load_user_index():
    """
    Devuelve un diccionario:
        { puuid: ultimo_riot_id }
    """
    mapping = {}

    for doc in coll_users.find({}, {"puuids": 1, "riotIds": 1}):
        puuids = doc.get("puuids", [])
        riot_ids = doc.get("riotIds", [])

        name = None
        if riot_ids:
            name = riot_ids[-1].split("#")[0]  # se queda solo con el nombre

        for p in puuids:
            mapping[p] = name or "(Sin nombre)"

    return mapping


PUUID_TO_NAME = load_user_index()


# ==============================================================
#                   FUNCIÓN PRINCIPAL
# ==============================================================

def load_match_summary(match_id: str, allowed_queues=None, min_amigos: int = 1):
    """
    Devuelve un JSON completo con todos los datos necesarios para renderizar.
    """

    doc = coll_matches.find_one({"_id": match_id})

    if not doc:
        return {"error": f"No existe la partida {match_id} en L0"}

    data = doc.get("data", {})
    info = data.get("info")
    metadata = data.get("metadata")

    if not info or not metadata:
        return {"error": "Partida corrupta o incompleta en L0"}

    queue_id = info.get("queueId")

    if allowed_queues is not None and queue_id not in allowed_queues:
        return {"error": f"Cola {queue_id} no permitida"}

    # Amigos mínimos
    participants_puuids = metadata.get("participants", [])
    if len(participants_puuids) < min_amigos:
        return {"error": f"Partida {match_id} no cumple min_amigos={min_amigos}"}

    # ==========================================================
    #    FUNCIÓN DE PROCESAR CADA JUGADOR
    # ==========================================================
    def build_player(p):
        puuid = p.get("puuid")

        real_name = PUUID_TO_NAME.get(puuid, p.get("summonerName") or "(Sin nombre)")

        farm = p.get("totalMinionsKilled", 0) + p.get("neutralMinionsKilled", 0)
        duration_minutes = max(1, info.get("gameDuration", 1) / 60)

        items = [
            p.get("item0"), p.get("item1"), p.get("item2"),
            p.get("item3"), p.get("item4"), p.get("item5"),
            p.get("item6"),
        ]

        perks = p.get("perks", {}).get("styles", [])

        primary = None
        secondary = None

        # ===========================
        #  RUNA PRIMARIA REAL
        # ===========================
        if len(perks) >= 1:
            selections = perks[0].get("selections", [])
            if selections:
                primary = selections[0].get("perk")   # OK → runa primaria

        # ===========================
        #  RAMA SECUNDARIA REAL (STYLE)
        # ===========================
        if len(perks) >= 2:
            secondary = perks[1].get("style") 

        return {
            "puuid": puuid,
            "name": real_name,
            "champ": p.get("championName"),
            "champLevel": p.get("champLevel"),

            "kills": p.get("kills", 0),
            "deaths": p.get("deaths", 0),
            "assists": p.get("assists", 0),
            "kda": round((p.get("kills", 0) + p.get("assists", 0)) / max(1, p.get("deaths", 0)), 2),

            "damage": p.get("totalDamageDealtToChampions", 0),
            "dpm": round(p.get("totalDamageDealtToChampions", 0) / duration_minutes, 1),

            "gold": p.get("goldEarned", 0),
            "gpm": round(p.get("goldEarned", 0) / duration_minutes, 1),

            "cs": farm,
            "cspm": round(farm / duration_minutes, 1),

            "visionScore": p.get("visionScore", 0),

            "summoner1Id": p.get("summoner1Id"),
            "summoner2Id": p.get("summoner2Id"),

            "primary": primary,
            "secondary": secondary,

            "items": items,

            "role": p.get("teamPosition"),
            "lane": p.get("lane"),

            "win": p.get("win", False),
        }

    # ==========================================================
    #           PROCESAR EQUIPOS
    # ==========================================================

    blue_players = []
    red_players = []

    for p in info.get("participants", []):
        entry = build_player(p)

        if p.get("teamId") == 100:
            blue_players.append(entry)
        else:
            red_players.append(entry)

    # Daño total por equipo (para damage share)
    blue_total_damage = sum(p["damage"] for p in blue_players)
    red_total_damage = sum(p["damage"] for p in red_players)

    for p in blue_players:
        p["damageShare"] = round(p["damage"] / max(1, blue_total_damage) * 100, 1)

    for p in red_players:
        p["damageShare"] = round(p["damage"] / max(1, red_total_damage) * 100, 1)

    # Team win
    teams_raw = info.get("teams", [])
    blue_win = next((t["win"] for t in teams_raw if t["teamId"] == 100), False)
    red_win = next((t["win"] for t in teams_raw if t["teamId"] == 200), False)

    duration = info.get("gameDuration", 0)
    timestamp = info.get("gameStartTimestamp")

    start_time = datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "matchId": match_id,
        "queueId": queue_id,
        "duration": duration,
        "start_time": start_time,
        "teams": {
            "blue": {
                "win": blue_win,
                "players": blue_players
            },
            "red": {
                "win": red_win,
                "players": red_players
            }
        }
    }
