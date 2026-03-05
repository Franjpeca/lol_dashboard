"""
Diagnóstico extendido: muestra estado de L0, L1 y PostgreSQL.
Uso: python diag.py
"""
import sys
from pathlib import Path

ROOT = Path("/home/dev/lol_dashboard")
sys.path.insert(0, str(ROOT / "src"))

from utils.config import MONGO_DB, POSTGRES_URI
from utils.db import get_mongo_client
import psycopg2

PG_DSN = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")

with get_mongo_client() as client:
    db = client[MONGO_DB]

    # ── 1. L0: cuántas partidas raw hay en total ────────────────────────────
    print("\n=== L0: Partidas crudas totales ===")
    l0_count = db["L0_all_raw_matches"].count_documents({})
    l0_flex  = db["L0_all_raw_matches"].count_documents({"data.info.queueId": 440})
    print(f"  Total L0_all_raw_matches : {l0_count}")
    print(f"  De las cuales son Flex   : {l0_flex}")

    # ── 2. PUUIDs del índice de usuarios normal ─────────────────────────────
    print("\n=== Usuarios en L0_users_index ===")
    puuids_normal = set()
    personas_normal = []
    for doc in db["L0_users_index"].find({}, {"persona": 1, "puuids": 1}):
        persona = doc.get("persona", "?")
        puuids = doc.get("puuids", [])
        puuids_normal.update(puuids)
        personas_normal.append(f"  {persona}: {len(puuids)} PUUIDs")
    print(f"  Total personas : {len(personas_normal)}")
    print(f"  Total PUUIDs   : {len(puuids_normal)}")
    for p in sorted(personas_normal):
        print(p)

    # ── 3. Cuántas partidas Flex tienen al menos 1 PUUID conocido ──────────
    print("\n=== Cobertura de PUUIDs en L0 Flex ===")
    match_counts = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    cursor = db["L0_all_raw_matches"].find(
        {"data.info.queueId": 440},
        {"data.metadata.participants": 1}
    )
    for doc in cursor:
        participants = doc.get("data", {}).get("metadata", {}).get("participants", [])
        friends_in_match = sum(1 for p in participants if p in puuids_normal)
        for threshold in [1, 2, 3, 4, 5]:
            if friends_in_match >= threshold:
                match_counts[threshold] += 1

    print(f"  Partidas con >= 1 amigo  : {match_counts[1]}")
    print(f"  Partidas con >= 2 amigos : {match_counts[2]}")
    print(f"  Partidas con >= 3 amigos : {match_counts[3]}")
    print(f"  Partidas con >= 4 amigos : {match_counts[4]}")
    print(f"  Partidas con >= 5 amigos : {match_counts[5]}")

    # ── 4. L1 collections ─────────────────────────────────────────────────
    print("\n=== Colecciones L1 en MongoDB ===")
    for col in sorted(c for c in db.list_collection_names() if c.startswith("L1_")):
        count = db[col].count_documents({})
        print(f"  {col:50s}  ({count} docs)")

# ── 5. PostgreSQL ──────────────────────────────────────────────────────────
print("\n=== PostgreSQL: player_performances por pool/friends_count ===")
conn = psycopg2.connect(PG_DSN)
cur = conn.cursor()
cur.execute("""
    SELECT pool_id, friends_count, COUNT(*) AS rows, COUNT(DISTINCT match_id) AS matches
    FROM player_performances
    GROUP BY pool_id, friends_count
    ORDER BY pool_id, friends_count;
""")
rows = cur.fetchall()
print(f"  {'pool_id':20s} {'friends_count':14s} {'rows':10s} {'matches':10s}")
for r in rows:
    print(f"  {str(r[0]):20s} {str(r[1]):14s} {str(r[2]):10s} {str(r[3]):10s}")
cur.close()
conn.close()
print("\n=== FIN ===\n")
