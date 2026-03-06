import pymongo
client = pymongo.MongoClient('mongodb://localhost:27017')
doc = client['lol_dashboard']['matches_raw'].find_one({'_id': 'EUW1_7185911307'})
if doc:
    for p in doc['data']['info']['participants']:
        if p.get('championName') == 'MonkeyKing':
            print({k: p.get(k) for k in ['puuid', 'summonerName', 'riotIdGameName', 'riotIdTagLine', 'championName']})
else:
    print("Match not found")
