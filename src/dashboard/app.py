import argparse
import sys
from pathlib import Path
import dash

import threading  # lo sigues usando para el pipeline general
import subprocess

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
from charts.chart_12_botlane_synergy import render as render_botlane_synergy, get_synergy_data, make_fig_player_synergy

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


def run_metrics_script(queue: int, min_friends: int, start: str | None = None, end: str | None = None) -> str:
    """
    Ejecuta metricsMain.py de forma sincrona con los parametros adecuados.
    Devuelve stdout+stderr para logs.
    """
    cmd = [
        sys.executable,
        str(BASE_DIR / "src" / "metrics" / "metricsMain.py"),
        "--queue", str(queue),
        "--min", str(min_friends),
    ]

    if start:
        cmd += ["--start", start]
    if end:
        cmd += ["--end", end]

    print("[METRICS] Ejecutando comando:")
    print(" ".join(cmd))

    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except Exception as e:
        print("[METRICS] Error ejecutando metricsMain.py:", e)
        return f"Error ejecutando metricsMain.py: {e}"

    print("[METRICS] STDOUT:")
    print(result.stdout)
    print("[METRICS] STDERR:")
    print(result.stderr)

    if result.returncode != 0:
        return "metricsMain.py termino con error. Revisa los logs del servidor."
    return "metricsMain.py se ejecuto correctamente."


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
        html.Div(id="metrics-status", style={"color": "cyan", "marginLeft": "12px"}),

        html.Div([
            html.H1("Villaquesitos.gg", style={"marginBottom": "5px"}),

            html.Div([
                html.Span("Min friends:"),
                dcc.Dropdown(
                    id="min-friends-dropdown",
                    options=[
                        {"label": "1", "value": 1},
                        {"label": "2", "value": 2},
                        {"label": "3", "value": 3},
                        {"label": "4", "value": 4},
                        {"label": "5", "value": 5}
                    ],
                    value=min_friends_default,
                    clearable=False,
                    style={"width": "120px"},
                ),

                html.Span("Start:", style={"marginLeft": "16px", "color": "white"}),
                dcc.DatePickerSingle(
                    id="start-date",
                    display_format="YYYY-MM-DD",
                    style={"backgroundColor": "#333", "color": "white"}
                ),

                html.Span("End:", style={"marginLeft": "12px", "color": "white"}),
                dcc.DatePickerSingle(
                    id="end-date",
                    display_format="YYYY-MM-DD",
                    style={"backgroundColor": "#333", "color": "white"}
                ),

                html.Button(
                    "Recargar metricas",
                    id="btn-run-metrics",
                    n_clicks=0,
                    style={
                        "marginLeft": "12px",
                        "padding": "6px 14px",
                        "backgroundColor": "#444",
                        "color": "white",
                        "border": "1px solid #666",
                        "borderRadius": "6px",
                    }
                ),
            ], style={
                "marginTop": "10px",
                "display": "flex",
                "alignItems": "center",
                "gap": "10px"
            }),

        ], style={
            "padding": "16px 24px",
            "backgroundColor": "#222",
            "color": "white",
            "borderBottom": "1px solid #444"
        }),

        dcc.Interval(id="log-refresh", interval=500, n_intervals=0),
        dcc.Tabs(id="tabs", value="tab-win", children=[
            dcc.Tab(label="Winrate y partidas", value="tab-win"),
            dcc.Tab(label="Estadisticas por persona", value="tab-player"),
            dcc.Tab(label="Índices", value="tab-indices"),
            dcc.Tab(label="Estadisticas por rol", value="tab-rol"),
            dcc.Tab(label="Records de jugadores", value="tab-record"),
            dcc.Tab(label="Ver partida", value="tab-view-match"),
            dcc.Tab(label="Config API Key", value="tab-api"),
        ], style={"backgroundColor": "#111"}),

        dcc.Loading(
            id="loading-main-content",
            type="default",
            children=html.Div(id="tab-content", style={"padding": "24px"})
        ),

    ], style={"backgroundColor": "black", "minHeight": "100vh"})

    # ============================================================
    #  CALLBACK PRINCIPAL: tabs + min_friends + boton metricas
    # ============================================================
    @app.callback(
        Output("tab-content", "children"),
        Output("metrics-status", "children"),
        Input("tabs", "value"),
        Input("min-friends-dropdown", "value"),  # Se añade el valor del dropdown
        Input("btn-run-metrics", "n_clicks"),
        State("start-date", "date"),
        State("end-date", "date"),
    )
    def update_tab_content(selected_tab, min_friends, n_clicks_run, start_date, end_date):
        status_msg = dash.no_update

        ctx = dash.callback_context
        triggered_id = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None

        if triggered_id == "btn-run-metrics" and n_clicks_run:
            start = start_date if start_date else None
            end = end_date if end_date else None

            # Ejecutar metricsMain.py de forma sincrona
            status_msg = run_metrics_script(queue, min_friends, start, end)

        components = []

        # Aquí se genera el contenido en base al valor de min_friends y el tab seleccionado
        if selected_tab == "tab-win":
            components += normalize_output(render_winrate(pool_id, queue, min_friends, start=start_date, end=end_date), "winrate")
            components += normalize_output(render_champions(pool_id, queue, min_friends, start=start_date, end=end_date), "champions")
            
            # Layout estático para la sección de frecuencia
            components.append(dcc.Graph(id="freq-global-graph"))
            components.append(
                html.Div([
                    html.Span("Selecciona jugador:", style={"fontWeight": "bold", "marginRight": "10px"}),
                    dcc.Dropdown(
                        id="freq-player-dropdown",
                        clearable=False,
                        style={"width": "240px"}
                    ),
                ], style={
                    "display": "flex",
                    "alignItems": "center",
                    "backgroundColor": "#222",
                    "padding": "16px",
                    "borderRadius": "8px",
                    "marginTop": "16px",
                    "marginBottom": "16px"
                })
            )
            components.append(html.Div(id="freq-player-graph-container"))
            
            components += normalize_output(render_streaks(pool_id, queue, min_friends, start=start_date, end=end_date), "streaks")

        elif selected_tab == "tab-player":
            components += normalize_output(render_stats(pool_id, queue, min_friends, start=start_date, end=end_date), "stats-persona")
            components += normalize_output(render_first_metrics(pool_id, queue, min_friends, start=start_date, end=end_date), "first-metrics")
            components += normalize_output(render_skills(pool_id, queue, min_friends, start=start_date, end=end_date), "skills")

        elif selected_tab == "tab-indices":
            components.append(
                html.Div([
                    html.Span("Minimo de partidas (Sinergia):", style={"fontWeight": "bold", "marginRight": "10px"}),
                    dcc.Slider(
                        id="min-games-synergy-slider",
                        min=0, max=50, step=1, value=0,
                        marks={i: str(i) for i in range(0, 51, 5)},
                    ),
                ], style={"marginBottom": "24px", "padding": "16px",
                          "backgroundColor": "#222", "borderRadius": "8px"})
            )
            # Contenedor para los gráficos de sinergia que se actualizarán con el slider
            initial_synergy_graphs = render_botlane_synergy(pool_id, queue, min_friends, min_games=0, start=start_date, end=end_date)
            components.append(html.Div(
                normalize_output(initial_synergy_graphs, "botlane-synergy"),
                id="botlane-synergy-graphs-container"))

            # --- Sección de sinergia por jugador ---
            components.append(html.Hr(style={"borderColor": "#444", "margin": "40px 0"}))
            components.append(
                html.Div([
                    html.Span("Ver sinergia por jugador:", style={"fontWeight": "bold", "marginRight": "10px"}),
                    dcc.Dropdown(
                        id="synergy-player-dropdown",
                        clearable=True,
                        placeholder="Selecciona un jugador...",
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
            components.append(html.Div(id="synergy-player-graph-container"))

            # El resto de los gráficos de la pestaña se añaden después
            # y no se verán afectados por el callback del slider de sinergia.
            # Se cargarán una vez al seleccionar la pestaña.
            components += normalize_output(render_ego(pool_id, queue, min_friends, start=start_date, end=end_date), "ego-index")
            components += normalize_output(render_troll(pool_id, queue, min_friends, start=start_date, end=end_date), "troll-index")

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
            components += normalize_output(render_record_stats(pool_id, queue, min_friends, start=start_date, end=end_date), "record")

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
            components.append(
                html.Div([
                    html.H3("Configurar API Key de Riot", style={"color": "white"}),

                    html.Div([
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

                html.Button(
                    "Ejecutar metricas completas",
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

                html.Div(id="pipeline-status", style={"marginTop": "15px", "color": "yellow"}),

                html.Div([
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
                ])

                ])
            )

        return html.Div(components, style={"display": "flex", "flexDirection": "column", "gap": "32px"}), status_msg

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
        State("start-date", "date"),
        State("end-date", "date"),
    )
    def update_rol_graphs(selected_role, min_friends, min_games, start_date, end_date):
        if selected_role is None:
            selected_role = "TOP"
        
        output = render_stats_by_rol(pool_id, queue, min_friends, selected_role, min_games=min_games, start=start_date, end=end_date)

        # Si la función de renderizado falla y devuelve None, no mostramos nada.
        if output is None:
            return html.Div("No hay datos disponibles para mostrar para este rol.", style={"padding": "20px"})
        else:
            return html.Div(
                normalize_output(output, f"stats-rol-{selected_role}"),
                style={"display": "flex", "flexDirection": "column", "gap": "32px"},
            )

    @app.callback(
        Output("freq-global-graph", "figure"),
        Output("freq-player-dropdown", "options"),
        Output("freq-player-dropdown", "value"),
        Input("min-friends-dropdown", "value"),
        Input("start-date", "date"),
        Input("end-date", "date"),
    )
    def update_freq_data(min_friends, start_date, end_date):
        freq_data = render_games_freq(pool_id, queue, min_friends, start=start_date, end=end_date)
        global_fig = freq_data.get("global", go.Figure())
        player_figs = freq_data.get("players", {})
        
        players = list(player_figs.keys())
        options = [{"label": p, "value": p} for p in players]
        value = players[0] if players else None
        
        return global_fig, options, value

    @app.callback(
        Output("freq-player-graph-container", "children"),
        Input("freq-player-dropdown", "value"),
        Input("min-friends-dropdown", "value"),
        Input("start-date", "date"),
        Input("end-date", "date"),
    )
    def update_freq_player_graph(selected_player, min_friends, start_date, end_date):
        if not selected_player:
            return html.Div("Selecciona un jugador para ver su frecuencia de partidas.")
            
        freq_data = render_games_freq(pool_id, queue, min_friends, start=start_date, end=end_date)
        player_figs = freq_data.get("players", {})
        fig = player_figs.get(selected_player, go.Figure())
        return dcc.Graph(figure=fig)

    @app.callback(
        Output("api-key-status", "children"),
        Input("btn-save-api", "n_clicks"),
        State("input-api-key", "value"),
        prevent_initial_call=True  # Evita que el callback se ejecute al inicio o cuando no se haga clic
    )
    def save_api_key(_, user_key):
        if not user_key:
            return "Introduce una clave valida."

        try:
            save_new_temp_key(user_key)
            return "API Key guardada correctamente. Durara 30h."
        except ValueError:
            return "La API Key no es valida o esta caducada."
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

        return f"Ejecutando pipeline con min_friends = {min_friends}..."

    @app.callback(
        Output("pipeline-log", "children"),
        Input("log-refresh", "n_intervals")
    )
    def update_pipeline_log(_):
        lines = []
        while not PIPELINE_QUEUE.empty():
            lines.append(PIPELINE_QUEUE.get())

        if not lines:
            return dash.no_update
        return "".join(lines)

    @app.callback(
        Output("botlane-synergy-graphs-container", "children"),
        Input("min-games-synergy-slider", "value"),
        Input("min-friends-dropdown", "value"),
        State("start-date", "date"),
        State("end-date", "date"),
    )
    def update_synergy_graphs(min_games, min_friends, start_date, end_date):
        # Este callback se dispara cuando se cambia el slider de sinergia
        # o el dropdown de min_friends.
        output = render_botlane_synergy(pool_id, queue, min_friends, min_games=min_games, start=start_date, end=end_date)

        if not output:
            return html.Div("No hay datos de sinergia para los filtros seleccionados.", style={"padding": "20px"})
        
        # El Div exterior no es necesario, normalize_output ya devuelve una lista de componentes
        return normalize_output(output, "botlane-synergy")

    @app.callback(
        Output("synergy-player-dropdown", "options"),
        Output("synergy-player-dropdown", "value"),
        Input("min-friends-dropdown", "value"),
        Input("start-date", "date"),
        Input("end-date", "date"),
    )
    def update_synergy_player_dropdown(min_friends, start_date, end_date):
        _, players = get_synergy_data(pool_id, queue, min_friends, start=start_date, end=end_date)
        options = [{"label": p, "value": p} for p in players]
        return options, None # Resetea el valor al cambiar los filtros

    @app.callback(
        Output("synergy-player-graph-container", "children"),
        Input("synergy-player-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        State("start-date", "date"),
        State("end-date", "date"),
    )
    def update_synergy_player_graph(selected_player, min_friends, start_date, end_date):
        if not selected_player:
            return html.Div("Selecciona un jugador para ver sus sinergias.", style={"padding": "10px"})
        df, _ = get_synergy_data(pool_id, queue, min_friends, start=start_date, end=end_date)
        fig = make_fig_player_synergy(df, selected_player)
        return dcc.Graph(figure=fig) if fig else html.Div("No hay datos para este jugador.")

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
