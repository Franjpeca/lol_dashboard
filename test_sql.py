import psycopg2
import pandas as pd
import sys
sys.path.append('src')
from utils.config import POSTGRES_URI

dsn = POSTGRES_URI.replace('postgresql+psycopg2://', 'postgresql://')
dsn = dsn.replace('localhost', '127.0.0.1')
conn = psycopg2.connect(dsn)

print("==== SEASON POOL, MIN_FRIENDS=5 ====")
pool = 'season'
q = 440
min_f = 5
day = '2026-03-01'

df_m = pd.read_sql(f"""
    SELECT match_id, game_start_at, cardinality(friends_present) as friends
    FROM matches
    WHERE pool_id = '{pool}' AND queue_id = {q} 
    AND cardinality(friends_present) >= {min_f}
    AND DATE(game_start_at) = '{day}'
""", conn)
print(f"Global matches on {day}:", len(df_m))

df_pp = pd.read_sql(f"""
    SELECT match_id, game_start_at, puuid
    FROM player_performances
    WHERE pool_id = '{pool}' AND queue_id = {q} AND persona = 'Fran'
    AND friends_count >= {min_f}
    AND DATE(game_start_at) = '{day}'
""", conn)
print(f"Fran matches on {day}:", len(df_pp))

print("Is there any PP match not in Global matches?")
print(set(df_pp.match_id) - set(df_m.match_id))

print("\n==== SEASON POOL, MIN_FRIENDS=1 ====")
min_f = 1
df_m1 = pd.read_sql(f"""
    SELECT match_id, game_start_at, cardinality(friends_present) as friends
    FROM matches
    WHERE pool_id = '{pool}' AND queue_id = {q} 
    AND cardinality(friends_present) >= {min_f}
    AND DATE(game_start_at) = '{day}'
""", conn)
print(f"Global matches on {day}:", len(df_m1))

df_pp1 = pd.read_sql(f"""
    SELECT match_id, game_start_at, puuid
    FROM player_performances
    WHERE pool_id = '{pool}' AND queue_id = {q} AND persona = 'Fran'
    AND friends_count >= {min_f}
    AND DATE(game_start_at) = '{day}'
""", conn)
print(f"Fran matches on {day}:", len(df_pp1))

conn.close()
