import os
from datetime import datetime, timezone
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB")

COLL_L1 = "L1_q440_min5_pool_ac89fa8d"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
coll = db[COLL_L1]


# ======================================================
# BUSCAR TODAS LAS PARTIDAS DE UN DÍA (L1)
# ======================================================
def find_matches_by_date(target_date_str="2025-05-15"):
    """
    Devuelve todos los matchId de L1 que fueron jugados exactamente ese día.
    """
    # Convertimos la fecha a rango de timestamps (en ms)
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    start_ts = int(target_date.timestamp() * 1000)
    end_ts = int(target_date.replace(hour=23, minute=59, second=59).timestamp() * 1000)

    print(f"Buscando partidas de L1 del día {target_date_str}")
    print(f"Rango timestamp: {start_ts} → {end_ts}\n")

    matches_found = []

    cursor = coll.find({}, {
        "_id": 1,
        "data.info.gameStartTimestamp": 1
    })

    for doc in cursor:
        match_id = doc["_id"]

        game_ts = (
            doc.get("data", {})
               .get("info", {})
               .get("gameStartTimestamp")
        )

        if not isinstance(game_ts, int):
            continue

        if start_ts <= game_ts <= end_ts:
            matches_found.append(match_id)

    return matches_found


# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    matches = find_matches_by_date("2025-05-15")

    print("\n=====================================")
    print(" Partidas de L1 jugadas el 2025-05-15")
    print("=====================================\n")

    if matches:
        for m in matches:
            print(" -", m)
    else:
        print("No hay partidas en esa fecha.")
