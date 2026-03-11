
import sys
from pathlib import Path
import psycopg2
import psycopg2.extras
import pandas as pd

# Add src to path
sys.path.insert(0, str(Path("z:/lol_dashboard/src")))
from utils.config import POSTGRES_URI

_PG_DSN = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://").replace("@localhost", "@127.0.0.1")

def q(sql, params=()):
    conn = psycopg2.connect(_PG_DSN)
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        return pd.DataFrame(cur.fetchall())

sql = """
SELECT role, champion_name, COUNT(*), SUM(CASE WHEN win THEN 1 ELSE 0 END) as wins
FROM player_performances 
WHERE persona = 'eduardo' 
GROUP BY role, champion_name 
ORDER BY role, COUNT(*) DESC;
"""
df = q(sql)
print("Raw data for eduardo:")
print(df.to_string())

role_map = {
    'TOP': 'Top', 'JUNGLE': 'Jungla', 'MIDDLE': 'Mid', 'BOTTOM': 'ADC', 'UTILITY': 'Support',
    'CARRY': 'ADC', 'SUPPORT': 'Support', 'DUO_CARRY': 'ADC', 'DUO_SUPPORT': 'Support', 'DUO': 'Support'
}
df['mapped_role'] = df['role'].map(lambda x: role_map.get(x, x))
collisions = df.groupby(['mapped_role', 'champion']).size().reset_index(name='count')
collisions = collisions[collisions['count'] > 1]

if not collisions.empty:
    print("\nCOLLISIONS DETECTED:")
    print(collisions.to_string())
else:
    print("\nNo role mapping collisions found for eduardo.")
