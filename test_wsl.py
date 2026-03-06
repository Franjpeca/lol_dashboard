import sys
from pathlib import Path

# Adjust path when running in WSL
sys.path.insert(0, "/home/dev/lol_dashboard/src")
from utils.db import get_pg_connection

min_friends = 5
day = "2026-02-26"

with get_pg_connection() as conn:
    with conn.cursor() as cur:
        print(f"==== POOL: season, MIN_FRIENDS: {min_friends}, DATE: {day} ====")
        
        cur.execute(f"SELECT COUNT(*) FROM matches WHERE pool_id='season' AND queue_id=440 AND DATE(game_start_at) = '{day}' AND cardinality(friends_present) >= {min_friends}")
        print("Global matches count:", cur.fetchone()[0])

        cur.execute(f"SELECT COUNT(*) FROM player_performances WHERE pool_id='season' AND queue_id=440 AND persona='Fran' AND DATE(game_start_at) = '{day}' AND friends_count >= {min_friends}")
        print("Fran matches count:", cur.fetchone()[0])
        
        cur.execute(f"SELECT match_id FROM matches WHERE pool_id='season' AND queue_id=440 AND DATE(game_start_at) = '{day}' AND cardinality(friends_present) >= {min_friends}")
        m_ids = set(r[0] for r in cur.fetchall())
        
        cur.execute(f"SELECT match_id FROM player_performances WHERE pool_id='season' AND queue_id=440 AND persona='Fran' AND DATE(game_start_at) = '{day}' AND friends_count >= {min_friends}")
        f_ids = set(r[0] for r in cur.fetchall())
        
        print("In Fran, not in Global:")
        for mid in (f_ids - m_ids):
            cur.execute(f"SELECT cardinality(friends_present) FROM matches WHERE match_id='{mid}'")
            res = cur.fetchone()
            print(f"  {mid} (Global friends_present = {res[0] if res else 'MISSING'})")
            
        print("In Global, not in Fran:")
        for mid in (m_ids - f_ids):
            print(f"  {mid}")
