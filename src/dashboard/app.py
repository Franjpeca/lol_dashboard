import argparse
import sys
from pathlib import Path
import dash


import threading


import plotly.io as pio
import plotly.graph_objs as go
from dash import Dash, html, dcc, Input, Output, State, ALL
from dash.development.base_component import Component

pio.templates.default = "plotly_dark"

HOST, PORT = "127.0.0.1", 8080

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.append(str(BASE_DIR / "src"))

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

from viewGame.render_match import render_match
from viewGame.loader import load_match_summary

from utils.api_key_manager import save_new_temp_key

from run import main as run_pipeline

from run import PIPELINE_QUEUE

def normalize_output(output, base_id_prefix: str):
    results = []
    base_id_prefix = str(base_id_prefix)

    if output is None:
        return results

    if isinstance(output, dict) and "fig" in output:
        fig = output["fig"]
        results.append(
            dcc.Graph(
                figure=fig,
                id={"type": "record-graph", "subid": base_id_prefix}
            )
        )
        return results

    if isinstance(output, go.Figure):
        results.append(
            dcc.Graph(
                figure=output,
                id={"type": "generic-graph", "subid": base_id_prefix}
            )
        )
        return results

    if isinstance(output, Component):
        return [output]

    if isinstance(output, (list, tuple)):
        for idx, item in enumerate(output):
            if isinstance(item, dict) and "fig" in item:
                fig = item["fig"]
                results.append(
                    dcc.Graph(
                        figure=fig,
                        id={"type": "record-graph", "subid": f"{base_id_prefix}-{idx}"}
                    )
                )
                continue

            if isinstance(item, go.Figure):
                results.append(
                    dcc.Graph(
                        figure=item,
                        id={"type": "generic-graph", "subid": f"{base_id_prefix}-{idx}"}
                    )
                )
                continue

        return results

    return results


def create_app(pool_id: str, queue: int, min_friends_default: int) -> Dash:
    app = Dash(
        __name__,
        requests_pathname_prefix="/dashlol/",
        routes_pathname_prefix="/dashlol/"
    )
    app.title = "Villaquesitos.gg"

    app.layout = html.Div([

        dcc.Store(id="copy-store"),
        html.Div(id="copy-msg", style={"color": "yellow", "marginLeft": "12px"}),

        html.Div([
            html.H1("Villaquesitos.gg", style={"marginBottom": "5px"}),

            html.Div([
                html.Span("Min friends:"),
                dcc.Dropdown(
                    id="min-friends-dropdown",
                    options=[
                        {"label": "3", "value": 3},
                        {"label": "4", "value": 4},
                        {"label": "5", "value": 5}
                    ],
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
            dcc.Tab(label="Ver partida", value="tab-view-match"),
            dcc.Tab(label="Config API Key", value="tab-api"),
        ], style={"backgroundColor": "#111"}),

        html.Div(id="tab-content", style={"padding": "24px"}),

    ], style={"backgroundColor": "black", "minHeight": "100vh"})


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

            freq_data = render_games_freq(pool_id, queue, min_friends)
            global_fig = freq_data.get("global")
            player_figs = freq_data.get("players", {})

            if global_fig is not None:
                components.append(
                    dcc.Graph(
                        figure=global_fig,
                        id="freq-global-graph"
                    )
                )

            players = list(player_figs.keys())

            if players:
                first_player = players[0]

                components.append(
                    html.Div([
                        html.Span("Selecciona jugador:", style={"fontWeight": "bold", "marginRight": "10px"}),
                        dcc.Dropdown(
                            id="freq-player-dropdown",
                            options=[{"label": p, "value": p} for p in players],
                            value=first_player,
                            clearable=False,
                            style={"width": "240px"}
                        ),
                    ], style={
                        "display": "flex",
                        "alignItems": "center",
                        "backgroundColor": "#222",
                        "padding": "16px",
                        "borderRadius": "8px",
                        "marginBottom": "16px"
                    })
                )

                components.append(html.Div(id="freq-player-graph-container"))

            components += normalize_output(render_streaks(pool_id, queue, min_friends), "streaks")

        elif selected_tab == "tab-player":
            components += normalize_output(render_stats(pool_id, queue, min_friends), "stats-persona")
            components += normalize_output(render_ego(pool_id, queue, min_friends), "ego-index")
            components += normalize_output(render_troll(pool_id, queue, min_friends), "troll-index")
            components += normalize_output(render_first_metrics(pool_id, queue, min_friends), "first-metrics")
            components += normalize_output(render_skills(pool_id, queue, min_friends), "skills")

        elif selected_tab == "tab-rol":
            components.append(
                html.Div([
                    html.Span("Selecciona rol:", style={"fontWeight": "bold", "marginRight": "10px"}),
                    dcc.Dropdown(
                        id="role-dropdown",
                        options=[{"label": r, "value": r} for r in ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]],
                        value="TOP",
                        clearable=False,
                        style={"width": "200px"},
                    ),
                ], style={"display": "flex", "alignItems": "center", "marginBottom": "24px",
                          "padding": "16px", "backgroundColor": "#222", "borderRadius": "8px"})
            )

            components.append(
                html.Div([
                    html.Span("Minimo de partidas:", style={"fontWeight": "bold", "marginRight": "10px"}),
                    dcc.Slider(
                        id="min-games-slider",
                        min=0, max=50, step=1, value=0,
                        marks={i: str(i) for i in range(0, 51, 5)},
                    ),
                ], style={"marginBottom": "24px", "padding": "16px",
                          "backgroundColor": "#222", "borderRadius": "8px"})
            )

            components.append(html.Div(id="rol-graphs-container"))

        elif selected_tab == "tab-record":
            components += normalize_output(render_record_stats(pool_id, queue, min_friends), "record")

        elif selected_tab == "tab-view-match":

            components.append(
                html.Div([

                    html.H3("Ver partida completa", style={"color": "white"}),

                    html.Div([

                        dcc.Dropdown(
                            id="match-server-dropdown",
                            options=[
                                {"label": "EUW", "value": "EUW1"},
                                {"label": "EUNE", "value": "EUN1"},
                                {"label": "NA", "value": "NA1"},
                                {"label": "KR", "value": "KR"},
                                {"label": "BR", "value": "BR1"},
                                {"label": "LAN", "value": "LA1"},
                                {"label": "LAS", "value": "LA2"},
                                {"label": "JP", "value": "JP1"},
                                {"label": "TR", "value": "TR1"},
                                {"label": "RU", "value": "RU"},
                                {"label": "OCE", "value": "OC1"},
                            ],
                            value="EUW1",
                            clearable=False,
                            style={"width": "130px"}
                        ),

                        dcc.Input(
                            id="match-id-input",
                            type="text",
                            placeholder="ID (solo numeros)",
                            style={"width": "180px", "marginLeft": "12px", "marginRight": "12px"}
                        ),

                        html.Button(
                            "Ver partida",
                            id="btn-view-match",
                            n_clicks=0,
                            style={
                                "padding": "6px 14px",
                                "backgroundColor": "#444",
                                "color": "white",
                                "border": "1px solid #666",
                                "borderRadius": "6px",
                            }
                        ),

                    ], style={
                        "display": "flex",
                        "alignItems": "center",
                        "gap": "10px",
                        "marginTop": "10px"
                    }),

                    html.Div(id="match-render-container", style={"marginTop": "30px"}),

                ], style={
                    "padding": "20px",
                    "backgroundColor": "#222",
                    "borderRadius": "8px"
                })
            )


        elif selected_tab == "tab-api":
            # Aquí solo necesitas un `append()` con un único objeto, que es un `html.Div()`
            components.append(
                html.Div([  # Solo un `html.Div` que contiene todos los elementos dentro
                    html.H3("Configurar API Key de Riot", style={"color": "white"}),

                    html.Div([  # Botones, inputs, etc. dentro de este div
                        dcc.Input(
                            id="input-api-key",
                            type="text",
                            placeholder="Introduce tu API Key...",
                            style={"width": "400px", "marginRight": "12px"}
                        ),
                        html.Button(
                            "Guardar API Key",
                            id="btn-save-api",
                            n_clicks=0,
                            style={"padding": "6px 14px"}
                        ),
                    ], style={"marginTop": "10px", "display": "flex", "alignItems": "center"}),

                    html.Div(id="api-key-status", style={"marginTop": "20px", "color": "yellow"}),

                    # El nuevo botón para ejecutar las métricas completas
                    html.Button(
                        "Ejecutar métricas completas",
                        id="btn-run-pipeline",
                        n_clicks=0,
                        style={
                            "marginTop": "30px",
                            "padding": "10px 18px",
                            "backgroundColor": "#28a745",
                            "color": "white",
                            "border": "none",
                            "borderRadius": "6px",
                            "cursor": "pointer"
                        }
                    ),

                    # Estado de ejecución del pipeline
                    html.Div(id="pipeline-status", style={"marginTop": "15px", "color": "yellow"}),

                    html.Div(
                        [
                            dcc.Interval(id="log-refresh", interval=500, n_intervals=0),
                            html.Pre(
                                id="pipeline-log",
                                style={
                                    "whiteSpace": "pre-wrap",
                                    "backgroundColor": "#111",
                                    "padding": "12px",
                                    "border": "1px solid #444",
                                    "borderRadius": "6px",
                                    "color": "white",
                                    "maxHeight": "400px",
                                    "overflowY": "scroll",
                                    "marginTop": "20px"
                                }
                            ),
                        ]
                    )

                ])  # Solo un `html.Div` en `append()`
            )
        return html.Div(components, style={"display": "flex", "flexDirection": "column", "gap": "32px"})


    @app.callback(
        Output("match-render-container", "children"),
        Input("btn-view-match", "n_clicks"),
        Input("match-server-dropdown", "value"),
        Input("match-id-input", "value"),
        prevent_initial_call=True,
    )
    def render_selected_match(_, server, short_id):
        if not short_id:
            return html.Div("Introduce una ID valida.", style={"color": "red", "padding": "10px"})

        full_id = f"{server}_{short_id}"

        data = load_match_summary(full_id)
        return render_match(data)


    @app.callback(
        Output("copy-store", "data"),
        Output("copy-msg", "children"),
        Input({"type": "record-graph", "subid": ALL}, "clickData"),
        prevent_initial_call=True,
    )
    def copy_record_id(clicks):
        for click in clicks:
            if click and "points" in click:
                match_id = click["points"][0]["customdata"]
                return match_id, f"Copiado ID: {match_id}"
        return None, ""


    app.clientside_callback(
        """
        function(data) {
            if (!data) return "";
            navigator.clipboard.writeText(data);
            return "";
        }
        """,
        Output("copy-msg", "n_clicks"),
        Input("copy-store", "data")
    )


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


    @app.callback(
        Output("freq-player-graph-container", "children"),
        Input("freq-player-dropdown", "value"),
        Input("min-friends-dropdown", "value"),
    )
    def update_freq_player_graph(selected_player, min_friends):
        freq_data = render_games_freq(pool_id, queue, min_friends)
        player_figs = freq_data.get("players", {})

        if not player_figs:
            return html.Div("No hay datos de frecuencia por jugador", style={"color": "red"})

        if not selected_player or selected_player not in player_figs:
            selected_player = list(player_figs.keys())[0]

        fig = player_figs[selected_player]

        return dcc.Graph(
            figure=fig,
            id=f"freq-player-graph-{selected_player}"
        )


    @app.callback(
        Output("api-key-status", "children"),
        Input("btn-save-api", "n_clicks"),
        State("input-api-key", "value"),
        prevent_initial_call=True
    )
    def save_api_key(_, user_key):
        if not user_key:
            return "Introduce una clave válida."

        try:
            save_new_temp_key(user_key)  # valida y guarda 30h
            return "API Key guardada correctamente. Durará 30h."
        except ValueError:
            return "La API Key no es válida o está caducada."
        except Exception as e:
            return f"Error inesperado: {e}"




    @app.callback(
        Output("pipeline-status", "children"),
        Input("btn-run-pipeline", "n_clicks"),
        State("min-friends-dropdown", "value"),
        prevent_initial_call=True
    )
    def execute_pipeline(_, min_friends):

        def worker():
            try:
                run_pipeline(min_friends)
            except Exception as e:
                print("[ERROR EN PIPELINE]", e)

        threading.Thread(target=worker).start()

        return f"🚀 Ejecutando pipeline con min_friends = {min_friends}..."



    @app.callback(
        Output("pipeline-log", "children"),
        Input("log-refresh", "n_intervals")
    )
    def update_pipeline_log(_):
        lines = []
        while not PIPELINE_QUEUE.empty():
            lines.append(PIPELINE_QUEUE.get())
        
        # Si la cola está vacía, no actualices el log, sino no_update
        if not lines:
            return dash.no_update
        return "".join(lines)

    
    return app

def main():
    parser = argparse.ArgumentParser(description="Villaquesitos.gg")
    parser.add_argument("--min", type=int, default=5)
    args = parser.parse_args()

    pool_id = "ac89fa8d"
    queue = 440
    min_friends = args.min

    app = create_app(pool_id, queue, min_friends)
    app.run(host=HOST, port=PORT, debug=False)


if __name__ == "__main__":
    main()
