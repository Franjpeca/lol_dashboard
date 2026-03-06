import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR / "src"))

from utils.db import get_pg_connection

min_friends = 5

with get_pg_connection() as conn:
    with conn.cursor() as cur:
        print(f"==== POOL: season, MIN_FRIENDS: {min_friends} ====")
        cur.execute(f"""
            SELECT DATE(game_start_at), COUNT(*) 
            FROM matches 
            WHERE pool_id='season' AND queue_id=440 AND cardinality(friends_present) >= {min_friends}
            GROUP BY DATE(game_start_at)
            ORDER BY COUNT(*) DESC
            LIMIT 5
        """)
        print("Top 5 days Global matches:")
        for row in cur.fetchall():
            print(row)

        print("\nTop 5 days Fran matches:")
        cur.execute(f"""
            SELECT DATE(game_start_at), COUNT(*) 
            FROM player_performances 
            WHERE pool_id='season' AND queue_id=440 AND persona='Fran' AND friends_count >= {min_friends}
            GROUP BY DATE(game_start_at)
            ORDER BY COUNT(*) DESC
            LIMIT 5
        """)
        for row in cur.fetchall():
            print(row)
            
        print("\nMatches for Fran on 2026-01-11:")
        cur.execute(f"""
            SELECT match_id, riot_id_name
            FROM player_performances 
            WHERE pool_id='season' AND queue_id=440 AND persona='Fran' AND friends_count >= {min_friends}
            AND DATE(game_start_at) = '2026-01-11'
        """)
        fran_matches = cur.fetchall()
        for row in fran_matches:
            print(row)
            
        print("\nGlobal matches on 2026-01-11:")
        cur.execute(f"""
            SELECT match_id
            FROM matches 
            WHERE pool_id='season' AND queue_id=440 AND cardinality(friends_present) >= {min_friends}
            AND DATE(game_start_at) = '2026-01-11'
        """)
        for row in cur.fetchall():
            print(row)
