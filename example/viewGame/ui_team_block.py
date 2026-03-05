from dash import html
from .ui_player_row import player_row

def team_block(team_name, win, players, patch):
    title_color = "#3ba55d" if win else "#b33a3a"
    result_text = "VICTORY" if win else "DEFEAT"

    # ---- ENCABEZADOS (alineados con tu grid) ----
    header_row = html.Div(
        className="player-row header-row",
        children=[
            html.Div("Campeón / Runas", className="header-cell"),
            html.Div("Jugador", className="header-cell"),
            html.Div("KDA", className="header-cell"),
            html.Div("Daño", className="header-cell"),
            html.Div("Oro", className="header-cell"),
            html.Div("CS", className="header-cell"),
            html.Div("Objetos", className="header-cell"),
        ],
        style={"marginBottom": "12px"}  # separarlo del primer jugador
    )

    return html.Div(
        className="team-block",
        children=[
            html.Div(
                className="team-title",
                children=f"{team_name} — {result_text}",
                style={"color": title_color},
            ),

            # ⬇️ Encabezado AQUÍ
            header_row,

            html.Div(
                className="team-table",
                children=[player_row(p, patch) for p in players],
            ),
        ]
    )
