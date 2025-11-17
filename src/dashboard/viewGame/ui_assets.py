from dash import html
from .dragon import (
    champion_square_url,
    spell_icon_url,
    rune_icon_url,
    rune_style_icon_url,
    item_icon_url,
    get_latest_patch
)


def champ_icon(champ: str, patch=None):
    patch = patch or get_latest_patch()
    return html.Img(
        src=champion_square_url(champ, patch),
        className="champ-icon",
        title=champ,
    )


def spell_icons(s1: int, s2: int, patch=None):
    patch = patch or get_latest_patch()
    return html.Div(
        [
            html.Img(
                src=spell_icon_url(s1, patch),
                className="spell-icon",
                title=str(s1),
            ),
            html.Img(
                src=spell_icon_url(s2, patch),
                className="spell-icon",
                title=str(s2),
            ),
        ],
        style={"display": "flex", "flexDirection": "column"},
    )


def rune_icons(primary: int, secondary: int, patch=None):
    return html.Div(
        [
            # Ícono de la runa principal
            html.Img(
                src=rune_icon_url(primary),
                className="rune-icon",
            ),
            # Ícono del estilo secundario (la rama completa)
            html.Img(
                src=rune_style_icon_url(secondary),
                className="rune-icon",
            ),
        ],
        style={"display": "flex", "flexDirection": "column"},
    )


def item_icons(items: list[int], patch=None):
    patch = patch or get_latest_patch()
    icons = []
    for it in items:
        if not it or it == 0:
            continue
        icons.append(
            html.Img(
                src=item_icon_url(it, patch),
                className="item-icon",
                title=str(it),
            )
        )
    return html.Div(icons, className="items-container")
