import requests
import streamlit as st

@st.cache_data(ttl=3600)
def get_latest_patch():
    try:
        url = "https://ddragon.leagueoflegends.com/api/versions.json"
        response = requests.get(url, timeout=5)
        return response.json()[0]
    except:
        return "14.1.1" # Fallback

def champion_square_url(champ_name: str, patch: str):
    if not champ_name: return ""
    return f"https://ddragon.leagueoflegends.com/cdn/{patch}/img/champion/{champ_name}.png"

def spell_icon_url(spell_id: int, patch: str):
    m = {1: "SummonerBoost", 3: "SummonerExhaust", 4: "SummonerFlash", 6: "SummonerHaste", 
         7: "SummonerHeal", 11: "SummonerSmite", 12: "SummonerTeleport", 14: "SummonerDot", 21: "SummonerBarrier"}
    name = m.get(spell_id, "SummonerFlash")
    return f"https://ddragon.leagueoflegends.com/cdn/{patch}/img/spell/{name}.png"

def item_icon_url(item_id: int, patch: str):
    if not item_id or item_id == 0: return ""
    return f"https://ddragon.leagueoflegends.com/cdn/{patch}/img/item/{item_id}.png"

def rune_icon_url(rune_id: int):
    return f"https://ddragon.canisback.com/img/perk-images/Styles/7200_Domination.png" # Placeholder

def rune_style_icon_url(style_id: int):
    return f"https://ddragon.canisback.com/img/perk-images/Styles/7200_Domination.png" # Placeholder
