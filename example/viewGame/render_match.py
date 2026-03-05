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
                            html.Div(
                                className="sub-item",
                                children=[
                                    html.Span("Cola:", className="sub-label"),
                                    html.Span(str(data["queueId"]), className="sub-value"),
                                ],
                            ),
                            html.Div(
                                className="sub-item",
                                children=[
                                    html.Span("Duración:", className="sub-label"),
                                    html.Span(f"{minutes}:{seconds:02d}", className="sub-value"),
                                ],
                            ),
                            html.Div(
                                className="sub-item",
                                children=[
                                    html.Span("Fecha:", className="sub-label"),
                                    html.Span(data["start_time"], className="sub-value"),
                                ],
                            ),
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
