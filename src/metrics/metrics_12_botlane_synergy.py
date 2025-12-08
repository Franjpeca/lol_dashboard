import os
import json
import argparse
import sys
from pathlib import Path
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime
from collections import defaultdict
import itertools

sys.stdout.reconfigure(encoding='utf-8')

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB", "lol_data")
DEFAULT_MIN = int(os.getenv("MIN_FRIENDS_IN_MATCH", "5"))
DEFAULT_QUEUE = int(os.getenv("QUEUE_FLEX", "440"))

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

USERS_INDEX = db["L0_users_index"]

RESULTS_ROOT = Path("data/results")
RUNTIME_ROOT = Path("data/runtime")


def load_puuid_to_user_mapping():
    mapping = {}
    cursor = USERS_INDEX.find({}, {"persona": 1, "puuids": 1})
    for doc in cursor:
        for puuid in doc.get("puuids", []):
            mapping[puuid] = doc["persona"]
    return mapping


def auto_select_l1(queue, min_friends):
    prefix = f"L1_q{queue}_min{min_friends}_"
    candidates = [c for c in db.list_collection_names() if c.startswith(prefix)]
    if not candidates:
        return None
    candidates.sort()
    return candidates[-1]


def extract_pool_from_l1(l1_name):
    return "pool_" + l1_name.split("_pool_", 1)[1]


def normalize(value, min_val, max_val):
    if max_val == min_val:
        return 0.0
    return (value - min_val) / (max_val - min_val)


def compute_botlane_synergy(coll_name, puuid_to_user, dataset_folder, start_date=None, end_date=None):
    coll_src = db[coll_name]
    
    query = {}
    if start_date and end_date:
        start_ts = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_ts = int(datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59).timestamp() * 1000)
        query["data.info.gameStartTimestamp"] = {"$gte": start_ts, "$lte": end_ts}

    # Usamos un cursor para no cargar todas las partidas en memoria.
    # find() devuelve un cursor, que iteramos directamente.
    matches_cursor = coll_src.find(query)
    
    # Contamos las partidas para dar feedback
    total_matches = db[coll_name].count_documents(query)
    print(f"[12] Se encontraron {total_matches} partidas para procesar.")

    # {('PlayerA', 'PlayerB'): {'wins': 10, 'games': 20, ...}}
    duo_stats = defaultdict(lambda: defaultdict(float))

    for match in matches_cursor:
        participants = match.get("data", {}).get("info", {}).get("participants", [])
        
        # Mapear puuid a participante para acceso rápido
        puuid_map = {p['puuid']: p for p in participants}
        
        # Identificar jugadores por equipo
        team100_puuids = {p['puuid'] for p in participants if p['teamId'] == 100 and p['puuid'] in puuid_to_user}
        team200_puuids = {p['puuid'] for p in participants if p['teamId'] == 200 and p['puuid'] in puuid_to_user}

        for team_puuids in [team100_puuids, team200_puuids]:
            adcs = [p for p in team_puuids if puuid_map[p].get('teamPosition') == 'BOTTOM']
            supports = [p for p in team_puuids if puuid_map[p].get('teamPosition') == 'UTILITY']

            # Si tenemos al menos un ADC y un Support, formamos las parejas
            if adcs and supports:
                for adc_puuid, support_puuid in itertools.product(adcs, supports):
                    adc_persona = puuid_to_user.get(adc_puuid)
                    support_persona = puuid_to_user.get(support_puuid)

                    if not adc_persona or not support_persona or adc_persona == support_persona:
                        continue

                    # Ordenar para tener una clave única ('PlayerA', 'PlayerB')
                    duo_key = tuple(sorted((adc_persona, support_persona)))
                    
                    adc_p = puuid_map[adc_puuid]
                    support_p = puuid_map[support_puuid]

                    duo_stats[duo_key]['games'] += 1
                    if adc_p['win']:
                        duo_stats[duo_key]['wins'] += 1
                    
                    # KDA
                    adc_deaths = adc_p.get('deaths', 0) if adc_p.get('deaths', 0) > 0 else 1
                    support_deaths = support_p.get('deaths', 0) if support_p.get('deaths', 0) > 0 else 1
                    duo_stats[duo_key]['kda'] += ((adc_p.get('kills', 0) + adc_p.get('assists', 0)) / adc_deaths) + \
                                                 ((support_p.get('kills', 0) + support_p.get('assists', 0)) / support_deaths)
                    
                    # Damage
                    duo_stats[duo_key]['damage'] += adc_p.get('totalDamageDealtToChampions', 0) + support_p.get('totalDamageDealtToChampions', 0)
                    
                    # Vision
                    duo_stats[duo_key]['vision'] += adc_p.get('visionScore', 0) + support_p.get('visionScore', 0)
                    
                    # Economy
                    duo_stats[duo_key]['gold'] += adc_p.get('goldEarned', 0) + support_p.get('goldEarned', 0)

    if not duo_stats:
        print("[12] No se encontraron dúos de botlane en las partidas analizadas.")
        return

    # Calcular promedios y winrate
    final_results = []
    for key, stats in duo_stats.items():
        games = stats['games']
        final_results.append({
            'duo': list(key),
            'games': int(games),
            'wins': int(stats['wins']),
            'winrate': stats['wins'] / games,
            'avg_kda': stats['kda'] / games,
            'avg_damage': stats['damage'] / games,
            'avg_vision': stats['vision'] / games,
            'avg_gold': stats['gold'] / games,
        })

    # Normalización
    if len(final_results) > 1:
        min_kda = min(d['avg_kda'] for d in final_results)
        max_kda = max(d['avg_kda'] for d in final_results)
        min_dmg = min(d['avg_damage'] for d in final_results)
        max_dmg = max(d['avg_damage'] for d in final_results)
        min_vis = min(d['avg_vision'] for d in final_results)
        max_vis = max(d['avg_vision'] for d in final_results)
        min_gold = min(d['avg_gold'] for d in final_results)
        max_gold = max(d['avg_gold'] for d in final_results)
    else: # Si solo hay un dúo, la normalización es 1.0
        min_kda, max_kda, min_dmg, max_dmg, min_vis, max_vis, min_gold, max_gold = [0]*8

    # Pesos de la fórmula
    #w = {'winrate': 0.4, 'kda': 0.2, 'damage': 0.2, 'vision': 0.1, 'economy': 0.1}
    w = {'winrate': 0.4, 'kda': 0.2, 'damage': 0.2, 'economy': 0.1}

    for r in final_results:
        # El winrate ya está normalizado entre 0 y 1
        r['winrate_norm'] = r['winrate']
        r['kda_norm'] = normalize(r['avg_kda'], min_kda, max_kda) if len(final_results) > 1 else 1.0
        r['damage_norm'] = normalize(r['avg_damage'], min_dmg, max_dmg) if len(final_results) > 1 else 1.0
        r['vision_norm'] = normalize(r['avg_vision'], min_vis, max_vis) if len(final_results) > 1 else 1.0
        r['economy_norm'] = normalize(r['avg_gold'], min_gold, max_gold) if len(final_results) > 1 else 1.0

        score = (w['winrate'] * r['winrate_norm'] +
                 w['kda'] * r['kda_norm'] +
                 w['damage'] * r['damage_norm'] +
                 w['vision'] * r['vision_norm'] +
                 w['economy'] * r['economy_norm'])
        
        r['botlane_score'] = score

    # Ordenar por el score final
    final_results.sort(key=lambda x: x['botlane_score'], reverse=True)

    output_json = {
        "source_L1": coll_name,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "start_date": start_date,
        "end_date": end_date,
        "botlane_synergy": final_results
    }

    dataset_folder.mkdir(parents=True, exist_ok=True)
    filename = f"metrics_12_botlane_synergy_{start_date}_to_{end_date}.json" if start_date and end_date else "metrics_12_botlane_synergy.json"
    json_path = dataset_folder / filename

    with json_path.open("w", encoding="utf-8") as f:
        json.dump(output_json, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description="Calcula la sinergia de botlanes.")
    parser.add_argument("--queue", type=int, default=DEFAULT_QUEUE)
    parser.add_argument("--min", type=int, default=DEFAULT_MIN)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    args = parser.parse_args()

    print(f"[12] Starting botlane synergy... using L1_q{args.queue}_min{args.min}")

    l1_name = auto_select_l1(args.queue, args.min)
    if not l1_name:
        print(f"[12] No L1 collection found for q{args.queue} min{args.min}")
        return

    pool_id = extract_pool_from_l1(l1_name)
    
    dataset_folder = RUNTIME_ROOT / pool_id / f"q{args.queue}" / f"min{args.min}" if args.start and args.end else RESULTS_ROOT / pool_id / f"q{args.queue}" / f"min{args.min}"

    puuid_to_user = load_puuid_to_user_mapping()
    compute_botlane_synergy(l1_name, puuid_to_user, dataset_folder, args.start, args.end)

    print(f"[12] Ended")


if __name__ == "__main__":
    main()