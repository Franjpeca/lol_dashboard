from dash import html
from .ui_player_row import player_row


def team_block(team_name, win, players, patch):
    title_color = "#3ba55d" if win else "#b33a3a"
    result_text = "VICTORY" if win else "DEFEAT"

    return html.Div(
        className="team-block",
        children=[
            html.Div(
                className="team-title",
                children=f"{team_name} — {result_text}",
                style={"color": title_color},
            ),

            html.Div(
                className="team-table",
                children=[player_row(p, patch) for p in players],
            ),
        ]
    )
