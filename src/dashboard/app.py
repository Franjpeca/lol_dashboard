import argparse
from pathlib import Path

import plotly.io as pio
import plotly.graph_objs as go
from dash import Dash, html, dcc, Input, Output
from dash.development.base_component import Component

pio.templates.default = "plotly_dark"

HOST, PORT = "127.0.0.1", 8080
BASE_DIR = Path(__file__).resolve().parents[2]

# ============================
# IMPORTS DE GRAFICOS
# ============================

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


# ============================
# FUNCION AUXILIAR FLEXIBLE
# ============================

def normalize_output(output, base_id_prefix: str):
    """
    Normaliza la salida de las funciones render_* para que siempre sea
    una lista de componentes Dash.

    Soporta:
    - Una sola figura Plotly -> un dcc.Graph
    - Una lista/tupla de figuras -> varios dcc.Graph
    - Un solo componente Dash -> una lista con ese componente
    - Una lista/tupla de componentes Dash
    """
    if output is None:
        return []

    # Un solo componente Dash
    if isinstance(output, Component):
        return [output]

    # Una sola figura Plotly
    if isinstance(output, go.Figure):
        return [dcc.Graph(figure=output, id=f"{base_id_prefix}-0")]

    # Iterables (lista o tupla)
    if isinstance(output, (list, tuple)):
        comps = []
        for idx, item in enumerate(output):
            if isinstance(item, Component):
                comps.append(item)
            elif isinstance(item, go.Figure):
                comps.append(dcc.Graph(figure=item, id=f"{base_id_prefix}-{idx}"))
            # Si el item no es ni figura ni componente, se ignora
        return comps

    # Caso por defecto: se intenta tratar como figura
    try:
        fig = go.Figure(output)
        return [dcc.Graph(figure=fig, id=f"{base_id_prefix}-0")]
    except Exception:
        return []


# ============================
# FUNCION PRINCIPAL DE APP
# ============================

def create_app(pool_id: str, queue: int, min_friends_default: int) -> Dash:
    app = Dash(__name__)
    app.title = "Dashboard Datos LoL"

    app.layout = html.Div(
        [
            # Cabecera
            html.Div(
                [
                    html.H1(
                        "Dashboard Datos LoL",
                        style={"marginBottom": "5px"},
                    ),
                    html.Div(
                        [
                            html.Span("Min friends: "),
                            dcc.Dropdown(
                                id="min-friends-dropdown",
                                options=[
                                    {"label": "4", "value": 4},
                                    {"label": "5", "value": 5},
                                ],
                                value=min_friends_default,
                                clearable=False,
                                style={"width": "120px"},
                            ),
                        ],
                        style={
                            "marginTop": "10px",
                            "display": "flex",
                            "alignItems": "center",
                            "gap": "10px",
                        },
                    ),
                ],
                style={
                    "padding": "16px 24px",
                    "backgroundColor": "#222",
                    "color": "white",
                    "borderBottom": "1px solid #444",
                },
            ),

            # Tabs
            dcc.Tabs(
                id="tabs",
                value="tab-win",
                children=[
                    dcc.Tab(label="Winrate y partidas", value="tab-win"),
                    dcc.Tab(label="Estadisticas por persona", value="tab-player"),
                    dcc.Tab(label="Estadisticas por rol", value="tab-rol"),
                    dcc.Tab(label="Records de jugadores", value="tab-record"),
                ],
                style={"backgroundColor": "#111"},
            ),

            # Contenido de la pestaña
            html.Div(id="tab-content", style={"padding": "24px"}),
        ],
        style={"backgroundColor": "#111", "minHeight": "100vh"},
    )

    # ============================
    # CALLBACK PRINCIPAL
    # ============================

    @app.callback(
        Output("tab-content", "children"),
        Input("tabs", "value"),
        Input("min-friends-dropdown", "value"),
    )
    def update_tab_content(selected_tab, min_friends):
        components = []

        # Tab: Winrate y partidas
        if selected_tab == "tab-win":
            print("Rendering tab-win")
            # Aqui puedes anadir las graficas de esta pestaña, una a una
            # Cada render_* puede devolver:
            # - una figura
            # - una lista de figuras
            # - componentes Dash directamente
            components += normalize_output(
                render_winrate(pool_id, queue, min_friends),
                "winrate",
            )
            components += normalize_output(
                render_champions(pool_id, queue, min_friends),
                "champions",
            )
            components += normalize_output(
                render_games_freq(pool_id, queue, min_friends),
                "games-freq",
            )

            print("\n\nmin_friends\n\n:", min_friends)
            components += normalize_output(
                render_streaks(pool_id, queue, min_friends),
                "streaks",
            )

        # Tab: Estadisticas por persona
        elif selected_tab == "tab-player":
            print("Rendering tab-player")
            components += normalize_output(
                render_stats(pool_id, queue, min_friends),
                "stats-persona",
            )
            components += normalize_output(
                render_ego(pool_id, queue, min_friends),
                "ego-index",
            )
            components += normalize_output(
                render_troll(pool_id, queue, min_friends),
                "troll-index",
            )
            components += normalize_output(
                render_first_metrics(pool_id, queue, min_friends),
                "first-metrics",
            )
            components += normalize_output(
                render_skills(pool_id, queue, min_friends),
                "skills",
            )

        # Tab: Estadisticas por rol
        elif selected_tab == "tab-rol":
            print("Rendering tab-rol")
            components += normalize_output(
                render_stats_by_rol(pool_id, queue, min_friends),
                "stats-rol",
            )

        # Tab: Records de jugadores
        elif selected_tab == "tab-record":
            print("Rendering tab-record")
            components += normalize_output(
                render_record_stats(pool_id, queue, min_friends),
                "records",
            )

        # Puedes envolver todo en una columna basica
        return html.Div(
            components,
            style={
                "display": "flex",
                "flexDirection": "column",
                "gap": "32px",
            },
        )

    return app


# ============================
# ENTRYPOINT CLI
# ============================

def main():
    parser = argparse.ArgumentParser(description="Dashboard Datos LoL")
    parser.add_argument(
        "--min",
        type=int,
        default=5,
        help="Valor de min_friends (por defecto 5)",
    )
    args = parser.parse_args()

    pool_id = "ac89fa8d"
    queue = 440
    min_friends = args.min

    app = create_app(pool_id, queue, min_friends)
    app.run(host=HOST, port=PORT, debug=False)


if __name__ == "__main__":
    main()