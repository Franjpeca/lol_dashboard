from dash import html
from .ui_assets import champ_icon, spell_icons, rune_icons, item_icons


def player_row(p, patch=None):



    champ = p.get("champ", "")
    name = p.get("name", "Unknown")
    role = p.get("role", "?")

    k = p.get("kills", 0)
    d = p.get("deaths", 0)
    a = p.get("assists", 0)
    kda_val = p.get("kda", "0.0")

    dmg = p.get("damage", 0)
    dmg_share = p.get("damageShare", 0)
    dpm = p.get("dpm", 0)
    cs = p.get("cs", 0)
    cspm = p.get("cspm", 0)
    gold = p.get("gold", 0)

    kda_str = f"{k}/{d}/{a} ({kda_val})"
    cspm_str = f"{cspm} cs/min"
    dmg_share_str = f"{dmg_share}%"

    s1 = p.get("summoner1Id", 0)
    s2 = p.get("summoner2Id", 0)

    def extract_runes(pdata):
        if "primary" in pdata and "secondary" in pdata:
            if pdata["primary"] and pdata["secondary"]:
                return pdata["primary"], pdata["secondary"]

        if "perks" in pdata and isinstance(pdata["perks"], dict):
            styles = pdata["perks"].get("styles", [])
            if len(styles) >= 2:
                primary_style = styles[0].get("style")
                secondary_style = styles[1].get("style")
                return primary_style, secondary_style

        if "perkPrimaryStyle" in pdata and "perkSubStyle" in pdata:
            return pdata["perkPrimaryStyle"], pdata["perkSubStyle"]

        return 0, 0

    primary, secondary = extract_runes(p)

    items = p.get("items", [])

    return html.Div(
        className="player-row",
        children=[
            html.Div(
                className="left-icons",
                children=[
                    champ_icon(champ, patch),
                    html.Div(
                        className="spells-runes",
                        children=[
                            html.Div(
                                className="spells",
                                children=spell_icons(s1, s2, patch),
                            ),
                            html.Div(
                                className="runes",
                                children=rune_icons(primary, secondary, patch),
                            ),
                        ],
                    ),
                ],
            ),
            html.Div(
                className="name-role",
                children=[
                    html.Div(className="player-name", children=name),
                    html.Div(className="player-role", children=role),
                ]
            ),
            html.Div(className="stat kda", children=kda_str),
            html.Div(
                className="stat damage",
                children=[
                    html.Div(f"{dmg:,}".replace(",", ".")),
                    html.Div(
                        className="bar damage-bar",
                        style={
                            "width": f"{min(100, dmg_share)}%",
                        },
                    ),
                    html.Div(className="small-text", children=dmg_share_str),
                ],
            ),
            html.Div(
                className="stat gold",
                children=f"{gold:,}".replace(",", "."),
            ),
            html.Div(
                className="stat cs",
                children=[
                    html.Div(f"{cs}"),
                    html.Div(className="small-text", children=cspm_str),
                ]
            ),
            html.Div(
                className="items",
                children=item_icons(items, patch),
            ),
        ],
    )
