from dash import html
from .ui_team_block import team_block
from .dragon import get_latest_patch


def render_match(data):
    if "error" in data:
        return html.Div(data["error"], style={"color": "red"})

    patch = get_latest_patch()

    blue = data["teams"]["blue"]
    red = data["teams"]["red"]

    duration = data["duration"]
    minutes = duration // 60
    seconds = duration % 60

    return html.Div(
        className="match-container",
        children=[
            # ENCABEZADO
            html.Div(
                className="match-header",
                children=[
                    html.H2(f"Partida {data['matchId']}"),
                    html.Div(
                        className="match-sub",
                        children=[
                            html.Span(f"Cola: {data['queueId']}  "),
                            html.Span(f"Duración: {minutes}:{seconds:02d}  "),
                            html.Span(f"Fecha: {data['start_time']}"),
                        ],
                    ),
                ],
            ),

            html.Div(
                className="teams-wrapper",
                children=[
                    team_block("BLUE TEAM", blue["win"], blue["players"], patch),
                    team_block("RED TEAM", red["win"], red["players"], patch),
                ],
            ),
        ]
    )
