import requests

_cached_patch = None
_cached_runes = None
_cached_styles = None

# ======================================================================
#  OBTENER ÚLTIMO PARCHE OFICIAL
# ======================================================================
def get_latest_patch():
    global _cached_patch
    if _cached_patch:
        return _cached_patch

    url = "https://ddragon.leagueoflegends.com/api/versions.json"
    try:
        resp = requests.get(url, timeout=3)
        resp.raise_for_status()
        versions = resp.json()
        _cached_patch = versions[0] if versions else "14.1.1"
    except:
        _cached_patch = "14.1.1"

    return _cached_patch


BASE = "https://ddragon.leagueoflegends.com/cdn"


# ======================================================================
#  CARGAR RUNAS INDIVIDUALES (PERKS)
# ======================================================================
def load_rune_metadata():
    global _cached_runes
    if _cached_runes:
        return _cached_runes

    patch = get_latest_patch()
    url = f"{BASE}/{patch}/data/en_US/runesReforged.json"

    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        raw = resp.json()
    except:
        raw = []

    mapping = {}

    # Cada "tree" es Precision/Domination/etc
    for tree in raw:
        for slot in tree["slots"]:
            for rune in slot["runes"]:
                rune_id = rune["id"]
                icon_path = rune["icon"]  # ejemplo: "perk-images/Styles/Domination/Electrocute/Electrocute.png"
                mapping[rune_id] = icon_path

    _cached_runes = mapping
    return mapping


# ======================================================================
#  CARGAR ICONOS DE LOS ESTILOS (RAMAS)
# ======================================================================
def load_rune_styles():
    global _cached_styles
    if _cached_styles:
        return _cached_styles

    patch = get_latest_patch()
    url = f"{BASE}/{patch}/data/en_US/runesReforged.json"

    try:
        resp = requests.get(url, timeout=5)
        resp.raise_for_status()
        raw = resp.json()
    except:
        raw = []

    mapping = {}

    # Cada entrada tiene un "id" y un icono de rama
    for tree in raw:
        style_id = tree["id"]          # ej 8000 = Precision
        icon_path = tree["icon"]       # ej "perk-images/Styles/Precision/Precision.png"
        mapping[style_id] = icon_path

    _cached_styles = mapping
    return mapping


# ======================================================================
#  ICONO DE RUNA PRIMARIA (individual)
# ======================================================================
def rune_icon_url(rune_id, patch=None):
    if not rune_id:
        return None

    metadata = load_rune_metadata()
    icon_path = metadata.get(rune_id)

    if not icon_path:
        return None

    return f"{BASE}/img/{icon_path}"


# ======================================================================
#  ICONO DE ESTILO (solo rama: Precision, Domination...)
# ======================================================================
def rune_style_icon_url(style_id):
    if not style_id:
        return None

    styles = load_rune_styles()
    icon_path = styles.get(style_id)

    if not icon_path:
        return None

    return f"{BASE}/img/{icon_path}"


# ======================================================================
#  ICONO DE CAMPEÓN
# ======================================================================
def champion_square_url(champion_name, patch=None):
    patch = patch or get_latest_patch()
    return f"{BASE}/{patch}/img/champion/{champion_name}.png"


# ======================================================================
#  ICONO DE HECHIZO
# ======================================================================
def spell_icon_url(spell_id, patch=None):
    if not spell_id:
        return None
    patch = patch or get_latest_patch()

    mapping = {
        1: "SummonerBoost",
        3: "SummonerExhaust",
        4: "SummonerFlash",
        6: "SummonerHaste",
        7: "SummonerHeal",
        11: "SummonerSmite",
        12: "SummonerTeleport",
        13: "SummonerMana",
        14: "SummonerDot",
        21: "SummonerBarrier",
        32: "SummonerSnowball",
    }

    name = mapping.get(spell_id, f"Summoner{spell_id}")
    return f"{BASE}/{patch}/img/spell/{name}.png"


# ======================================================================
#  ICONO DE ITEM
# ======================================================================
def item_icon_url(item_id, patch=None):
    if not item_id:
        return None
    patch = patch or get_latest_patch()
    return f"{BASE}/{patch}/img/item/{item_id}.png"
