import argparse
from pathlib import Path

import plotly.io as pio
import plotly.graph_objs as go
from dash import Dash, html, dcc, Input, Output
from dash.development.base_component import Component

pio.templates.default = "plotly_dark"

HOST, PORT = "127.0.0.1", 8080
BASE_DIR = Path(__file__).resolve().parents[2]

from charts.chart_01_metrics_players_games_winrate import render as render_winrate
from charts.chart_02_metrics_champions_games_winrate import render as render_champions
from charts.chart_03_metrics_games_frecuency import render as render_games_freq
from charts.chart_04_metrics_win_lose_streak import render as render_streaks
from charts.chart_05_metrics_players_stats import render as render_stats
from charts.chart_06_metrics_ego_index import render as render_ego
from charts.chart_07_metrics_troll_index import render as render_troll
from charts.chart_08_metrics_first_metrics import render as render_first_metrics
from charts.chart_09_metrics_number_skills import render as render_skills
from charts.chart_10_metrics_stats_by_rol import render as render_stats_by_rol
from charts.chart_11_metrics_stats_record import render as render_record_stats

def normalize_output(output, base_id_prefix: str):
    if output is None:
        return []

    if isinstance(output, Component):
        return [output]

    if isinstance(output, go.Figure):
        return [dcc.Graph(figure=output, id=f"{base_id_prefix}-0")]

    if isinstance(output, (list, tuple)):
        comps = []
        for idx, item in enumerate(output):
            if isinstance(item, Component):
                comps.append(item)
            elif isinstance(item, go.Figure):
                comps.append(dcc.Graph(figure=item, id=f"{base_id_prefix}-{idx}"))
        return comps

    try:
        fig = go.Figure(output)
        return [dcc.Graph(figure=fig, id=f"{base_id_prefix}-0")]
    except Exception:
        return []

def create_app(pool_id: str, queue: int, min_friends_default: int) -> Dash:
    app = Dash(__name__)
    app.title = "🧀 Villaquesitos.gg "

    app.layout = html.Div([
        html.Div([
            html.H1("🧀 Villaquesitos.gg ", style={"marginBottom": "5px"}),

            html.Div([
                html.Span("Min friends:"),
                dcc.Dropdown(
                    id="min-friends-dropdown",
                    options=[{"label": "4", "value": 4}, {"label": "5", "value": 5}],
                    value=min_friends_default,
                    clearable=False,
                    style={"width": "120px"},
                ),
            ], style={"marginTop": "10px", "display": "flex", "alignItems": "center", "gap": "10px"}),
        ], style={"padding": "16px 24px", "backgroundColor": "#222", "color": "white", "borderBottom": "1px solid #444"}),

        dcc.Tabs(id="tabs", value="tab-win", children=[
            dcc.Tab(label="Winrate y partidas", value="tab-win"),
            dcc.Tab(label="Estadisticas por persona", value="tab-player"),
            dcc.Tab(label="Estadisticas por rol", value="tab-rol"),
            dcc.Tab(label="Records de jugadores", value="tab-record"),
        ], style={"backgroundColor": "#111"}),

        html.Div(id="tab-content", style={"padding": "24px"}),
    ], style={"backgroundColor": "#111", "minHeight": "100vh"})

    @app.callback(
        Output("tab-content", "children"),
        Input("tabs", "value"),
        Input("min-friends-dropdown", "value"),
    )
    def update_tab_content(selected_tab, min_friends):
        components = []

        if selected_tab == "tab-win":
            components += normalize_output(render_winrate(pool_id, queue, min_friends), "winrate")
            components += normalize_output(render_champions(pool_id, queue, min_friends), "champions")
            components += normalize_output(render_games_freq(pool_id, queue, min_friends), "games-freq")
            components += normalize_output(render_streaks(pool_id, queue, min_friends), "streaks")

        elif selected_tab == "tab-player":
            components += normalize_output(render_stats(pool_id, queue, min_friends), "stats-persona")
            components += normalize_output(render_ego(pool_id, queue, min_friends), "ego-index")
            components += normalize_output(render_troll(pool_id, queue, min_friends), "troll-index")
            components += normalize_output(render_first_metrics(pool_id, queue, min_friends), "first-metrics")
            components += normalize_output(render_skills(pool_id, queue, min_friends), "skills")

        elif selected_tab == "tab-rol":
            rol_selector = html.Div([
                html.Span("Selecciona rol:", style={"fontWeight": "bold", "marginRight": "10px"}),
                dcc.Dropdown(
                    id="role-dropdown",
                    options=[{"label": r, "value": r} for r in ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]],
                    value="TOP",
                    clearable=False,
                    style={"width": "200px"},
                ),
            ], style={"display": "flex", "alignItems": "center", "marginBottom": "24px", "padding": "16px", "backgroundColor": "#222", "borderRadius": "8px"})

            min_games_slider = html.Div([
                html.Span("Minimo de partidas:", style={"fontWeight": "bold", "marginRight": "10px"}),
                dcc.Slider(
                    id="min-games-slider",
                    min=0,
                    max=50,
                    step=1,
                    value=0,
                    marks={i: str(i) for i in range(0, 51, 5)},
                    tooltip={"placement": "bottom", "always_visible": False},
                ),
            ], style={"marginBottom": "24px", "padding": "16px", "backgroundColor": "#222", "borderRadius": "8px"})

            graphs_container = html.Div(id="rol-graphs-container")

            components.append(rol_selector)
            components.append(min_games_slider)
            components.append(graphs_container)

        elif selected_tab == "tab-record":
            components += normalize_output(render_record_stats(pool_id, queue, min_friends), "records")

        return html.Div(components, style={"display": "flex", "flexDirection": "column", "gap": "32px"})

    @app.callback(
        Output("rol-graphs-container", "children"),
        Input("role-dropdown", "value"),
        Input("min-friends-dropdown", "value"),
        Input("min-games-slider", "value"),
    )
    def update_rol_graphs(selected_role, min_friends, min_games):
        if selected_role is None:
            selected_role = "TOP"

        return html.Div(
            normalize_output(
                render_stats_by_rol(pool_id, queue, min_friends, selected_role, min_games=min_games),
                f"stats-rol-{selected_role}",
            ),
            style={"display": "flex", "flexDirection": "column", "gap": "32px"},
        )

    return app

def main():
    parser = argparse.ArgumentParser(description="🧀 Villaquesitos.gg ")
    parser.add_argument("--min", type=int, default=5)
    args = parser.parse_args()

    pool_id = "ac89fa8d"
    queue = 440
    min_friends = args.min

    app = create_app(pool_id, queue, min_friends)
    app.run(host=HOST, port=PORT, debug=False)

if __name__ == "__main__":
    main()