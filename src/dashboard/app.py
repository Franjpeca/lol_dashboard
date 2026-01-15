
import argparse
import sys
import threading
import subprocess
from pathlib import Path
from typing import List
import json

import dash
from dash import Dash, html, dcc, Input, Output, State, ALL, callback_context
from dash.development.base_component import Component
import plotly.io as pio
import plotly.graph_objs as go

# Set Plotly template
pio.templates.default = "plotly_dark"

HOST, PORT = "127.0.0.1", 8080

# Setup paths
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parents[2]        # lol_data/
SRC_DIR = BASE_DIR / "src"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

# Import Charts & Utils
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
from charts.chart_13_player_champions_stats import render as render_player_champions_stats

from viewGame.render_match import render_match
from viewGame.loader import load_match_summary

from utils.api_key_manager import save_new_temp_key
from utils.pool_manager import get_available_pools, get_available_reports
from utils.data_checker import check_data_availability

from charts.chart_utils import safe_render

from run_pipeline import run_l0_only, run_l1_to_l3, main_full as run_full_pipeline
from queue import Queue

PIPELINE_QUEUE = Queue()  # Cola para enviar salida a Dash

# Helper
def normalize_output(output, base_id_prefix: str):
    results = []
    base_id_prefix = str(base_id_prefix)

    if output is None:
        return results

    if isinstance(output, dict) and "fig" in output:
        fig = output["fig"]
        results.append(
            html.Div(
                dcc.Graph(
                    figure=fig,
                    id={"type": "record-graph", "subid": base_id_prefix}
                ),
                className="graph-card"
            )
        )
        return results

    if isinstance(output, go.Figure):
        results.append(
            html.Div(
                dcc.Graph(
                    figure=output,
                    id={"type": "generic-graph", "subid": base_id_prefix}
                ),
                className="graph-card"
            )
        )
        return results

    if isinstance(output, Component):
        return [output]

    if isinstance(output, (list, tuple)):
        # Recursively handle lists but wrap in card if it's a fig
        for idx, item in enumerate(output):
            if isinstance(item, dict) and "fig" in item:
                fig = item["fig"]
                results.append(
                    html.Div(
                        dcc.Graph(
                            figure=fig,
                            id={"type": "record-graph", "subid": f"{base_id_prefix}-{idx}"}
                        ),
                        className="graph-card"
                    )
                )
                continue

            if isinstance(item, go.Figure):
                results.append(
                    html.Div(
                        dcc.Graph(
                            figure=item,
                            id={"type": "generic-graph", "subid": f"{base_id_prefix}-{idx}"}
                        ),
                        className="graph-card"
                    )
                )
                continue
            
            # If it's a component or something else, append as is
            if isinstance(item, Component):
                results.append(item)

        return results

    return results


def run_metrics_script(pool_id: str, queue: int, min_friends: int, start: str | None = None, end: str | None = None) -> str:
    """
    Runs metricsMain.py synchronously.
    """
    cmd = [
        sys.executable,
        str(BASE_DIR / "src" / "metrics" / "metricsMain.py"),
        "--pool", str(pool_id),
        "--queue", str(queue),
        "--min", str(min_friends),
    ]

    if start:
        cmd += ["--start", start]
    if end:
        cmd += ["--end", end]

    print("[METRICS] Executing:", " ".join(cmd))

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(BASE_DIR))
    except Exception as e:
        print("[METRICS] Error:", e)
        return f"Error executing metricsMain.py: {e}"

    print("[METRICS] STDOUT:", result.stdout)
    print("[METRICS] STDERR:", result.stderr)

    if result.returncode != 0:
        return "metricsMain.py ended with error. Check server logs."
    return "metricsMain.py executed successfully."


def create_app() -> Dash:
    app = Dash(
        __name__,
        requests_pathname_prefix="/dashlol/",
        routes_pathname_prefix="/dashlol/",
        suppress_callback_exceptions=True,
        serve_locally=False  # Force fresh layout, no cache
    )
    app.title = "Villaquesitos.gg"
    
    # Season Mode Constants
    SEASON_POOL_ID = "season"
    SEASON_START_DATE = "2026-01-08"
    from datetime import date
    SEASON_END_DATE = date.today().isoformat()

    # Initial Data
    available_pools = get_available_pools(BASE_DIR)
    
    if available_pools:
        default_pool = available_pools[0]
    else:
        default_pool = None
    
    queue = 440  # Hardcoded as in original, could be made dynamic later

    app.layout = html.Div([
        
        # URL Location for detecting season route
        dcc.Location(id="url", refresh=False),
        dcc.Store(id="season-mode-store", data=False),  # True if on /dashlol/season

        dcc.Store(id="copy-store"),
        html.Div(id="copy-msg", style={"position": "fixed", "top": "10px", "right": "10px", "zIndex": "1000", "color": "#00d2ff", "fontWeight": "bold"}),
        html.Div(id="metrics-status", style={"display": "none"}), # Hidden storage or status
        dcc.Store(id="pipeline-status-store", data="IDLE"), # IDLE, RUNNING, ERROR, SUCCESS
        dcc.Store(id="report-trigger-store"),  # Stores timestamp when report generation starts
        dcc.Interval(id="report-refresh-interval", interval=1000, n_intervals=0),  # Check every second

        # --- Header ---
        html.Div(id="header-container", className="header-container", children=[
            
            # LEFT SIDE: Logo and Navigation
            html.Div(className="header-side", children=[
                html.Div("Villaquesitos.gg", className="logo-text"),
                
                # Separator bar
                html.Div(style={
                    "width": "1px",
                    "height": "30px",
                    "backgroundColor": "rgba(255, 255, 255, 0.15)",
                }),
                
                # Navigation buttons
                html.Div(id="btn-go-season", children=[
                    dcc.Link(
                        "Ir a datos de la temporada",
                        className="btn-nav",
                        href="/dashlol/season",
                        style={"backgroundColor": "#ffd700", "color": "#000"}
                    )
                ]),
                html.Div(id="btn-go-normal", children=[
                    dcc.Link(
                        "Ir a datos completos",
                        className="btn-nav",
                        href="/dashlol/",
                        style={"backgroundColor": "#00d2ff", "color": "#000"}
                    )
                ], style={"display": "none"}),
            ]),
            
            # CENTER: Season Indicator
            html.Div(className="header-center", children=[
                html.Div(id="season-indicator", children=[
                    html.Span("Season 2026", className="season-title-big"),
                    # Date removed as requested
                ], style={"display": "none"}),
            ]),

            # RIGHT SIDE: Configuration Controls
            html.Div(className="header-right", children=[
                html.Div(id="pool-ctrl", children=[
                    html.Label("Pool:", className="control-label"),
                    dcc.Dropdown(
                        id="pool-dropdown",
                        options=[], # Populated by callback
                        value=default_pool,
                        clearable=False,
                        placeholder="Seleccionar...",
                        className="dash-dropdown",
                        style={"width": "180px"}
                    )
                ], className="control-group"),

                # Last Updated Label (Left of Min Friends)
                html.Div(id="last-updated-label", style={
                    "fontSize": "0.75em", 
                    "color": "#aaa", 
                    "marginRight": "15px", 
                    "fontStyle": "italic",
                    "display": "flex",
                    "alignItems": "center"
                }),

                html.Div(id="min-friends-ctrl", children=[
                    html.Label("Mín. Amigos:", className="control-label"),
                    dcc.Dropdown(
                        id="min-friends-dropdown",
                        options=[{"label": str(i), "value": i} for i in range(1, 6)],
                        value=5,
                        clearable=False,
                        placeholder="...",
                        style={"width": "70px"},
                        className="dash-dropdown"
                    )
                ], className="control-group"),

                html.Div(id="report-ctrl", children=[
                    html.Label("Datos completos / por fechas:", className="control-label"),
                    dcc.Dropdown(
                        id="report-dropdown",
                        options=[], # Populated by callback
                        value="all",
                        clearable=False,
                        placeholder="Seleccionar...",
                        className="dash-dropdown",
                        style={"width": "230px"}
                    )
                ], className="control-group"),
            ]),

        ]),

        dcc.Interval(id="log-refresh", interval=1000, n_intervals=0),

        # --- Tabs ---
        html.Div([
            # Data Alert
            html.Div(id="data-alert-container", style={"margin": "20px"}),
            
            dcc.Tabs(id="tabs", value="tab-win", className="custom-tabs-container", parent_className="custom-tabs", children=[
                dcc.Tab(label="Winrate y Partidas", value="tab-win", className="custom-tab", selected_className="custom-tab--selected"),
                dcc.Tab(label="Estadísticas de Jugador", value="tab-player", className="custom-tab", selected_className="custom-tab--selected"),
                dcc.Tab(label="Índices", value="tab-indices", className="custom-tab", selected_className="custom-tab--selected"),
                dcc.Tab(label="Estadísticas por Rol", value="tab-rol", className="custom-tab", selected_className="custom-tab--selected"),
                dcc.Tab(label="Récords", value="tab-record", className="custom-tab", selected_className="custom-tab--selected"),
                dcc.Tab(label="Ver Partida", value="tab-view-match", className="custom-tab", selected_className="custom-tab--selected"),
                dcc.Tab(label="Datos y Configuración", value="tab-api", className="custom-tab", selected_className="custom-tab--selected"),
            ]),
        ]),

        # --- Content ---
        # --- Content ---
        html.Div(id="tab-content", style={"padding": "24px", "maxWidth": "1600px", "margin": "0 auto"}),

    ], style={"backgroundColor": "#050505", "minHeight": "100vh"})

    # --- Callbacks ---
    
    # Callback to detect season mode from URL
    @app.callback(
        Output("season-mode-store", "data"),
        Input("url", "pathname")
    )
    def detect_season_mode(pathname):
        if pathname and "season" in pathname:
            return True
        return False
    
    # Callback to toggle visibility of controls based on season mode
    @app.callback(
        Output("pool-ctrl", "style"),
        Output("min-friends-ctrl", "style"),
        Output("report-ctrl", "style"),
        Output("season-indicator", "style"),
        Output("btn-go-season", "style"),
        Output("btn-go-normal", "style"),
        Input("season-mode-store", "data")
    )
    def toggle_controls_visibility(is_season_mode):
        if is_season_mode:
            # Season mode: Hide Pool, Show Friends, HIDE Reports, Show Indicator
            return (
                {"display": "none"},                                    # Pool hidden
                {"display": "flex"},                                    # Friends shown
                {"display": "none"},                                    # Report HIDDEN (Requested by user)
                {"display": "flex", "alignItems": "center", "gap": "10px"}, # Indicator shown
                {"display": "none"},                                    # Go Season hidden
                {"display": "inline-block"}                             # Go Normal shown
            )
        else:
            # Normal mode: Show all controls, Hide Indicator
            return (
                {"display": "flex"},                                    # Pool shown
                {"display": "flex"},                                    # Friends shown
                {"display": "flex"},                                    # Report shown
                {"display": "none"},                                    # Indicator hidden
                {"display": "inline-block"},                            # Go Season shown
                {"display": "none"}                                     # Go Normal hidden
            )
    
    # Callback to populate Report Dropdown based on Pool/Min Friends
    @app.callback(
        Output("report-dropdown", "options"),
        Output("report-dropdown", "value"),
        Input("pool-dropdown", "value"),
        Input("min-friends-dropdown", "value"),
        State("report-dropdown", "value"), # Preserve current selection if valid
        Input("season-mode-store", "data")
    )
    def update_report_options(pool_id, min_friends, current_val, is_season_mode):
        if is_season_mode:
            pool_id = SEASON_POOL_ID
        
        if not pool_id:
             return [{"label": "Datos completos", "value": "all"}], "all"
        
        reports = get_available_reports(BASE_DIR, pool_id, queue, min_friends)
        
        valid_values = [r["value"] for r in reports]
        new_val = current_val if current_val in valid_values else "all"
        
        return reports, new_val

    # Callback to refresh pool dropdown options
    @app.callback(
        Output("pool-dropdown", "options"),
        Input("tabs", "value"),
        Input("pipeline-status-store", "data"),
    )
    def refresh_pool_dropdown(tab, pipeline_status):
        pools = get_available_pools(BASE_DIR)
        options = [{"label": p if p != "auto" else "Auto (crear nuevo)", "value": p} for p in pools]
        
        # Always ensure 'auto' is an option for new pool creation
        if not any(opt["value"] == "auto" for opt in options):
            options.insert(0, {"label": "Auto (crear nuevo)", "value": "auto"})
            
        return options

    @app.callback(
        Output("tab-content", "children"),
        Output("metrics-status", "children"),
        Output("last-updated-label", "children"),
        Input("tabs", "value"),
        Input("pool-dropdown", "value"),
        Input("min-friends-dropdown", "value"),
        Input("report-dropdown", "value"),
        Input("season-mode-store", "data"),
    )
    def update_tab_content(selected_tab, pool_id, min_friends, report_id, is_season_mode):
        # Override values in season mode
        if is_season_mode:
            pool_id = SEASON_POOL_ID
            # min_friends and report_id come from inputs, so we respect them!
            # If report_id is not set by user interaction yet (initial load), it might be "all" or None.
            # But the report-dropdown options will be updated by its own callback to include season dates.
        
        if not pool_id and selected_tab != "tab-api":

            return html.Div([
                html.H3("No se ha seleccionado Pool", style={"textAlign": "center", "marginTop": "50px"}),
                html.P("Por favor, selecciona un Pool en el desplegable superior o ve a la pestaña 'Datos y Configuración' para generar uno nuevo.", style={"textAlign": "center", "color": "#888"})
            ]), dash.no_update, ""
        
        # Check Data Availability
        data_exists = check_data_availability(pool_id, queue, min_friends)
        
        # Parse Dates from report_id
        start_date = None
        end_date = None
        
        if report_id and report_id != "all" and "|" in report_id:
            try:
                start_date, end_date = report_id.split("|")
            except:
                pass
        
        # We always return content, but if missing data triggers an alert, we can show specific msg
        alert_content = None
        if not data_exists:
            alert_content = html.Div([
                html.H4(f"⚠️ No se encontraron datos para el Pool '{pool_id}' (Mín. Amigos: {min_friends})"),
                html.P("Ve a la pestaña 'Datos y Configuración' y ejecuta el pipeline para generar métricas."),
            ], style={"backgroundColor": "#330000", "color": "#ffcccc", "padding": "15px", "borderRadius": "8px", "border": "1px solid #ff0000"})
        else:
            alert_content = None

        ctx = callback_context
        # triggered_id = ctx.triggered[0]["prop_id"].split(".")[0] if ctx.triggered else None
        status_msg = dash.no_update

        components = [alert_content] if alert_content else []

        if selected_tab == "tab-win":
            components += normalize_output(safe_render(render_winrate, pool_id, queue, min_friends, start=start_date, end=end_date), "winrate")
            components += normalize_output(safe_render(render_champions, pool_id, queue, min_friends, start=start_date, end=end_date), "champions")
            
            # Custom Section for Champions by Player
            components.append(html.Div([
                html.H3("Campeones por Jugador"),
                dcc.Dropdown(
                    id="player-champions-dropdown",
                    clearable=True,
                    placeholder="Seleccionar Jugador...",
                    style={"width": "300px"},
                    className="dash-dropdown"
                ),
                dcc.Loading(html.Div(id="player-champions-graph-container"))
            ], className="graph-card"))

            # Freq Game
            components.append(html.Div([
                html.H3("Frecuencia de Partidas"),
                html.Div(id="freq-global-container"),
                html.Div([
                    html.Span("Desglose por jugador:"),
                    dcc.Dropdown(id="freq-player-dropdown", clearable=False, style={"width":"250px"}, className="dash-dropdown")
                ], style={"marginTop": "15px", "display": "flex", "gap": "10px", "alignItems": "center"}),
                dcc.Loading(html.Div(id="freq-player-graph-container"))
            ], className="graph-card"))

            components += normalize_output(safe_render(render_streaks, pool_id, queue, min_friends, start=start_date, end=end_date), "streaks")

        elif selected_tab == "tab-player":
            components += normalize_output(safe_render(render_stats, pool_id, queue, min_friends, start=start_date, end=end_date), "stats-persona")
            components += normalize_output(safe_render(render_first_metrics, pool_id, queue, min_friends, start=start_date, end=end_date), "first-metrics")
            components += normalize_output(safe_render(render_skills, pool_id, queue, min_friends, start=start_date, end=end_date), "skills")

        elif selected_tab == "tab-indices":
            components.append(html.Div([
                html.H3("Sinergia Botlane"),
                html.Label("Mín. Partidas:"),
                dcc.Slider(
                    id="min-games-synergy-slider", 
                    min=0, max=50, step=1, value=0,
                    marks={i: str(i) for i in range(0, 51, 5)}
                ),
                html.Div(id="botlane-synergy-graphs-container", style={"marginTop": "20px"}),
                
                html.Hr(style={"borderColor": "#333", "margin": "30px 0"}),
                
                html.H4("Sinergia por Jugador"),
                dcc.Dropdown(id="synergy-player-dropdown", placeholder="Seleccionar Jugador...", style={"width":"300px"}, className="dash-dropdown"),
                html.Div(id="synergy-player-graph-container", style={"marginTop": "20px"})
            ], className="graph-card"))

            components += normalize_output(safe_render(render_ego, pool_id, queue, min_friends, start=start_date, end=end_date), "ego-index")
            components += normalize_output(safe_render(render_troll, pool_id, queue, min_friends, start=start_date, end=end_date), "troll-index")

        elif selected_tab == "tab-rol":
            # Gráfica 2: Stats por Rol (ya existe)
            components.append(html.Div([
                html.Div([
                    html.Label("Rol:", style={"marginRight": "10px"}),
                    dcc.Dropdown(
                        id="role-dropdown",
                        options=[{"label": r, "value": r} for r in ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]],
                        value="TOP",
                        clearable=False,
                        style={"width": "200px"},
                        className="dash-dropdown"
                    ),
                ], style={"marginBottom": "20px"}),
                html.Label("Mín. Partidas:"),
                dcc.Slider(
                    id="min-games-slider",
                    min=0, max=50, step=1, value=0,
                    marks={i: str(i) for i in range(0, 51, 5)}
                ),
                html.Div(id="rol-graphs-container", style={"marginTop": "20px"})
            ], className="graph-card"))
            
            # # Gráfica 3: Comparación Cross-Rol (COMENTADO - No implementado)
            # components.append(html.Div([
            #     html.H3("🔀 Comparación Cross-Rol", style={"marginBottom": "10px"}),
            #     html.P("Compara jugadores de diferentes roles usando escala global",
            #            style={"color": "#888", "fontSize": "0.9em", "marginBottom": "20px"}),
            #     
            #     # Selector Jugador 1
            #     html.Div([
            #         html.Label("Jugador 1:", style={"marginRight": "10px", "fontWeight": "bold"}),
            #         dcc.Dropdown(
            #             id="cross-role-player1",
            #             placeholder="Seleccionar jugador...",
            #             clearable=True,
            #             style={"width": "200px", "marginRight": "15px"},
            #             className="dash-dropdown"
            #         ),
            #         html.Label("Rol:", style={"marginRight": "10px"}),
            #         dcc.Dropdown(
            #             id="cross-role-role1",
            #             options=[{"label": r, "value": r} for r in ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]],
            #             placeholder="Seleccionar rol...",
            #             clearable=True,
            #             style={"width": "150px"},
            #             className="dash-dropdown"
            #         ),
            #     ], style={"display": "flex", "alignItems": "center", "marginBottom": "15px"}),
            #     
            #     # Selector Jugador 2
            #     html.Div([
            #         html.Label("Jugador 2:", style={"marginRight": "10px", "fontWeight": "bold"}),
            #         dcc.Dropdown(
            #             id="cross-role-player2",
            #             placeholder="Seleccionar jugador... (opcional)",
            #             clearable=True,
            #             style={"width": "200px", "marginRight": "15px"},
            #             className="dash-dropdown"
            #         ),
            #         html.Label("Rol:", style={"marginRight": "10px"}),
            #         dcc.Dropdown(
            #             id="cross-role-role2",
            #             options=[{"label": r, "value": r} for r in ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]],
            #             placeholder="Seleccionar rol...",
            #             clearable=True,
            #             style={"width": "150px"},
            #             className="dash-dropdown"
            #         ),
            #     ], style={"display": "flex", "alignItems": "center", "marginBottom": "20px"}),
            #     
            #     # Botón generar
            #     html.Button(
            #         "Generar Comparación",
            #         id="btn-generate-cross-role",
            #         className="btn-primary",
            #         style={"marginBottom": "20px"}
            #     ),
            #     
            #     # Contenedor del gráfico
            #     dcc.Loading(html.Div(id="cross-role-graph-container"))
            # ], className="graph-card"))


        elif selected_tab == "tab-record":
            components += normalize_output(safe_render(render_record_stats, pool_id, queue, min_friends, start=start_date, end=end_date), "record")

        elif selected_tab == "tab-view-match":
            components.append(html.Div([
                html.H3("Visor de Partidas"),
                html.Div([
                    dcc.Dropdown(
                        id="match-server-dropdown",
                        options=[
                            {"label": "EUW", "value": "EUW1"},
                            {"label": "EUNE", "value": "EUN1"},
                            {"label": "NA", "value": "NA1"},
                            {"label": "KR", "value": "KR"},
                        ],
                        value="EUW1",
                        clearable=False,
                        className="dash-dropdown",
                        style={"width": "100px"}
                    ),
                    dcc.Input(
                        id="match-id-input",
                        type="text",
                        placeholder="ID de Partida",
                        className="input-dark",
                        style={"width": "200px"}
                    ),
                    html.Button("Cargar Partida", id="btn-view-match", className="btn-primary")
                ], style={"display": "flex", "gap": "15px", "alignItems": "center", "marginBottom": "20px"}),
                
                dcc.Loading(html.Div(id="match-render-container"))
            ], className="graph-card"))

        elif selected_tab == "tab-api":
            components.append(html.Div([
                html.H3("Configuración de Datos"),
                
                # Three-column layout
                html.Div([
                    # LEFT: API Key
                    html.Div([
                        html.H4("Clave API de Riot", style={"marginTop": "0"}),
                        html.Div(id="current-key-status", style={"marginBottom": "10px", "fontSize": "0.9em"}),
                        html.P("Introduce tu clave API de Riot y espera 2 minutos antes de pulsar el botón:", style={"color": "#888", "fontSize": "0.9em"}),
                        dcc.Input(
                            id="input-api-key", 
                            placeholder="RGAPI-...", 
                            className="input-dark", 
                            style={"width": "100%", "marginBottom": "10px"}
                        ),
                        html.Button("Guardar Clave", id="btn-save-api", className="btn-primary", style={
                            "width": "100%",
                            "height": "45px",
                            "backgroundColor": "#00d2ff",
                            "fontSize": "1em",
                            "fontWeight": "600",
                            "marginTop": "auto" # Push to bottom
                        }),
                        html.Div(id="api-key-status", style={"marginTop": "10px", "fontSize": "0.9em"}),
                        
                        html.Details([
                            html.Summary("¿Cómo obtener mi clave?", style={"color": "#00d2ff", "cursor": "pointer", "fontSize": "0.9em", "marginTop": "10px"}),
                            html.Div([
                                html.Div("Entra en esta web, loguéate con tu cuenta y genera una nueva clave, cópiala y ponla aquí.", style={"marginTop": "8px"}),
                                html.Div([
                                    "Enlace a la web: ",
                                    html.A("https://developer.riotgames.com/", href="https://developer.riotgames.com/", target="_blank", style={"color": "#00d2ff", "textDecoration": "underline"})
                                ], style={"marginTop": "4px"})
                            ], style={"color": "#aaa", "fontSize": "0.85em", "lineHeight": "1.3"})
                        ], style={"marginTop": "5px"})
                    ], style={
                        "display": "flex", "flexDirection": "column", 
                        "flex": "1", 
                        # minHeight removed to avoid extra gaps inside
                        "padding": "20px", 
                        "backgroundColor": "#1a1a1a", 
                        "borderRadius": "12px", 
                        "border": "1px solid #444"
                    }),
                    
                    # CENTER: Full Pipeline
                    html.Div([
                        html.H4("Pipeline Completo", style={"marginTop": "0"}),
                        html.P([
                            "Descarga partidas y ejecuta todas las métricas del pool y mín. amigos seleccionado:",
                            html.Br(),
                            "(Debe de haber una clave API valida)"
                        ], style={"color": "#aaa", "fontSize": "0.9em"}),
                        html.Ul([
                            html.Li("Descargar nuevas partidas (L0)", style={"color": "#888", "fontSize": "0.85em"}),
                            html.Li("Procesar y filtrar datos (L1-L2)", style={"color": "#888", "fontSize": "0.85em"}),
                            html.Li("Generar métricas de todo el tiempo (L3)", style={"color": "#888", "fontSize": "0.85em"}),
                        ], style={"marginLeft": "20px", "marginBottom": "15px"}),
                        html.Button(
                            "Ejecutar Pipeline Completo (L0 → L3)", 
                            id="btn-run-pipeline", 
                            className="btn-primary", 
                            style={
                                "backgroundColor": "#00d2ff", 
                                "width": "100%",
                                "height": "45px",
                                "fontSize": "1em",
                                "fontWeight": "600",
                                "marginTop": "15px" # Consistent margin
                            }
                        ),
                        html.Div(id="pipeline-status", style={"marginTop": "10px", "fontSize": "0.9em"}),
                        html.Div(id="pipeline-alert", style={"marginTop": "5px"})
                    ], style={
                        "display": "flex", "flexDirection": "column", 
                        "flex": "1", 
                        # minHeight removed
                        "padding": "20px", 
                        "backgroundColor": "#1a1a1a", 
                        "borderRadius": "12px", 
                        "border": "1px solid #444"
                    }),
                    
                    # RIGHT: Custom Date Report
                    html.Div([
                        html.H4("Reporte por Fechas", style={"marginTop": "0"}),
                        html.P("Igual que el pipeline completo, pero establece un intervalo de fechas específico:", style={"color": "#aaa", "fontSize": "0.9em"}),
                        html.Label("Seleccionar rango de fechas:", style={"color": "#ddd", "fontSize": "0.9em", "marginBottom": "5px", "display": "block"}),
                        dcc.DatePickerRange(
                            id="date-range-config",
                            display_format="YYYY-MM-DD",
                            start_date_placeholder_text="Fecha inicio",
                            end_date_placeholder_text="Fecha fin",
                            style={"marginBottom": "15px", "width": "100%", "height": "40px"}
                        ),
                        html.Button(
                            "Generar Reporte", 
                            id="btn-generate-report", 
                            className="btn-primary", 
                            style={
                                "backgroundColor": "#00d2ff", 
                                "width": "100%",
                                "height": "45px",
                                "fontSize": "1em",
                                "fontWeight": "600",
                                "marginTop": "15px" # Consistent margin
                            }
                        ),
                        html.Div(id="report-generation-status", style={"marginTop": "10px", "fontSize": "0.9em", "fontWeight": "bold"})
                    ], style={
                        "display": "none" if is_season_mode else "flex", "flexDirection": "column", 
                        "flex": "1", 
                        # minHeight removed
                        "padding": "20px", 
                        "backgroundColor": "#1a1a1a", 
                        "borderRadius": "12px", 
                        "border": "1px solid #444"
                    }),
                    
                ], style={
                    "display": "flex", 
                    "gap": "20px", 
                    "marginBottom": "30px"
                }),
                
                # BOTTOM: Pipeline Logs
                html.Div([
                    html.H4("Registros del Pipeline", style={"marginTop": "0"}),
                    html.Div(html.Pre(id="pipeline-log"), className="pipeline-log-container")
                ])
            ], className="graph-card"))

        # Determine Last Updated Text
        last_updated_text = ""
        try:
            runtime_dir = BASE_DIR / "data" / "runtime" / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}"
            filename = "metrics_01_players_games_winrate.json"
            if start_date and end_date:
                filename = f"metrics_01_players_games_winrate_{start_date}_to_{end_date}.json"
            
            file_path = runtime_dir / filename
            if file_path.exists():
                with open(file_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                    ts = meta.get("generated_at", "")
                    if ts:
                        # ts is "YYYY-MM-DD HH:MM:SS"
                        # Shorten it if needed, or keep as is.
                        # Removing seconds might be cleaner: "2026-01-14 15:30"
                        if len(ts) > 16:
                            ts = ts[:16]
                        last_updated_text = f"Última act: {ts}"
        except Exception:
            pass

        return html.Div(components, style={"display": "flex", "flexDirection": "column", "gap": "20px"}), status_msg, last_updated_text

    # --- Match Viewer Callback ---
    @app.callback(
        Output("match-render-container", "children"),
        Input("btn-view-match", "n_clicks"),
        State("match-server-dropdown", "value"),
        State("match-id-input", "value"),
        prevent_initial_call=True,
    )
    def render_selected_match(_, server, short_id):
        if not short_id:
            return html.Div("ID inválido", style={"color": "red"})
        full_id = f"{server}_{short_id}"
        try:
            data = load_match_summary(full_id)
            return render_match(data)
        except Exception as e:
            return html.Div(f"Error al cargar la partida: {e}", style={"color": "red"})

    # --- Copy ID Callback ---
    @app.callback(
        Output("copy-store", "data"),
        Input({"type": "record-graph", "subid": ALL}, "clickData"),
        prevent_initial_call=True,
    )
    def copy_record_id(clicks):
        for click in clicks:
            if click and "points" in click:
                return click["points"][0].get("customdata", "")
        return dash.no_update

    app.clientside_callback(
        """
        function(data) {
            if (!data) return "";
            navigator.clipboard.writeText(data);
            return "Copied: " + data;
        }
        """,
        Output("copy-msg", "children"),
        Input("copy-store", "data")
    )

    # --- Role Graphs Callback ---
    @app.callback(
        Output("rol-graphs-container", "children"),
        Input("role-dropdown", "value"),
        Input("min-games-slider", "value"),
        Input("report-dropdown", "value"),
        State("pool-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        State("season-mode-store", "data"),
    )
    def update_rol_graphs_cb(role, min_games, report_id, pool_id, min_friends, is_season_mode):
        if is_season_mode: pool_id = SEASON_POOL_ID
        if not pool_id: return dash.no_update
        
        start_date = None
        end_date = None
        
        if report_id and report_id != "all" and "|" in report_id:
            try:
                start_date, end_date = report_id.split("|")
            except:
                pass
        
        try:
            output = safe_render(render_stats_by_rol, pool_id, queue, min_friends, role or "TOP", min_games=min_games, start=start_date, end=end_date)
            if output is None or not output: 
                from charts.chart_utils import create_empty_message
                return create_empty_message("Role Stats")
            return html.Div(normalize_output(output, f"stats-rol-{role}"), style={"display":"flex", "flexDirection":"column", "gap": "20px"})
        except Exception as e:
            from charts.chart_utils import create_empty_message
            return create_empty_message("Role Stats", "contiene errores")

    # --- Freq Graph Callback ---
    @app.callback(
        Output("freq-global-container", "children"),
        Output("freq-player-dropdown", "options"),
        Output("freq-player-dropdown", "value"),
        Input("pool-dropdown", "value"),
        Input("min-friends-dropdown", "value"),
        Input("tabs", "value"),
        Input("report-dropdown", "value"),
        State("season-mode-store", "data"),
    )
    def update_freq_data_cb(pool_id, min_friends, tab, report_id, is_season_mode):
        if is_season_mode: pool_id = SEASON_POOL_ID
        if tab != "tab-win" or not pool_id: return dash.no_update, dash.no_update, dash.no_update
        
        start_date = None
        end_date = None
        
        if report_id and report_id != "all" and "|" in report_id:
            try:
                start_date, end_date = report_id.split("|")
            except:
                pass
        
        try:
            freq_data = render_games_freq(pool_id, queue, min_friends, start=start_date, end=end_date)
            if not freq_data:
                from charts.chart_utils import create_empty_message
                return create_empty_message("Global Frequency", "no tiene datos disponibles"), [], None
            
            global_fig = freq_data.get("global", None)
            if global_fig:
                global_content = dcc.Graph(figure=global_fig)
            else:
                 from charts.chart_utils import create_empty_message
                 global_content = create_empty_message("Global Frequency", "no tiene datos disponibles")

            players = list(freq_data.get("players", {}).keys())
            options = [{"label": p, "value": p} for p in players]
            return global_content, options, (players[0] if players else None)
        except Exception as e:
             print("[FREQ ERROR]", e)
             from charts.chart_utils import create_empty_message
             return create_empty_message("Global Frequency", f"error: {type(e).__name__}"), [], None


    @app.callback(
        Output("freq-player-graph-container", "children"),
        Input("freq-player-dropdown", "value"),
        Input("report-dropdown", "value"),
        State("pool-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        State("season-mode-store", "data"),
    )
    def update_freq_player_graph_cb(player, report_id, pool_id, min_friends, is_season_mode):
        if is_season_mode: pool_id = SEASON_POOL_ID
        if not player or not pool_id: return html.Div()
        
        start_date = None
        end_date = None
        
        if report_id and report_id != "all" and "|" in report_id:
            try:
                start_date, end_date = report_id.split("|")
            except:
                pass
        
        try:
            freq_data = render_games_freq(pool_id, queue, min_friends, start=start_date, end=end_date)
            fig = freq_data.get("players", {}).get(player)
            return dcc.Graph(figure=fig) if fig else html.Div("No data")
        except Exception as e:
            from charts.chart_utils import create_empty_message
            return create_empty_message("Player Frequency", "contiene errores")

    # --- Synergy Callbacks ---
    @app.callback(
        Output("botlane-synergy-graphs-container", "children"),
        Input("min-games-synergy-slider", "value"),
        Input("report-dropdown", "value"),
        State("pool-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        Input("tabs", "value"),
        State("season-mode-store", "data"),
    )
    def update_synergy_graphs_cb(min_games, report_id, pool_id, min_friends, tab, is_season_mode):
        if is_season_mode: pool_id = SEASON_POOL_ID
        if tab != "tab-indices" or not pool_id: return dash.no_update
        
        start_date = None
        end_date = None
        if report_id and report_id != "all" and "|" in report_id:
            try:
                start_date, end_date = report_id.split("|")
            except:
                pass

        try:
            output = safe_render(render_botlane_synergy, pool_id, queue, min_friends, min_games=min_games, start=start_date, end=end_date)
            return normalize_output(output, "botlane-synergy") if output else html.Div("No data")
        except Exception as e:
            from charts.chart_utils import create_empty_message
            return create_empty_message("Botlane Synergy", "contiene errores")

    @app.callback(
        Output("synergy-player-dropdown", "options"),
        Output("synergy-player-dropdown", "value"),
        Input("report-dropdown", "value"),
        State("pool-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        Input("tabs", "value"),
        State("season-mode-store", "data"),
    )
    def update_synergy_player_dropdown_cb(report_id, pool_id, min_friends, tab, is_season_mode):
        if is_season_mode: pool_id = SEASON_POOL_ID
        if tab != "tab-indices" or not pool_id: return dash.no_update, dash.no_update
        
        start_date = None
        end_date = None
        if report_id and report_id != "all" and "|" in report_id:
            try:
                start_date, end_date = report_id.split("|")
            except:
                pass
        
        _, players = get_synergy_data(pool_id, queue, min_friends, start=start_date, end=end_date)
        return [{"label": p, "value": p} for p in players], None

    @app.callback(
        Output("synergy-player-graph-container", "children"),
        Input("synergy-player-dropdown", "value"),
        Input("report-dropdown", "value"),
        State("pool-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        State("season-mode-store", "data"),
    )
    def update_synergy_player_graph_cb(player, report_id, pool_id, min_friends, is_season_mode):
        if is_season_mode: pool_id = SEASON_POOL_ID
        if not player or not pool_id: return html.Div()
        
        start_date = None
        end_date = None
        if report_id and report_id != "all" and "|" in report_id:
            try:
                start_date, end_date = report_id.split("|")
            except:
                pass

        df, _ = get_synergy_data(pool_id, queue, min_friends, start=start_date, end=end_date)
        fig = make_fig_player_synergy(df, player)
        return dcc.Graph(figure=fig) if fig else html.Div("No data")

    # --- Player Champions Callbacks ---
    @app.callback(
        Output("player-champions-dropdown", "options"),
        Output("player-champions-dropdown", "value"),
        Input("report-dropdown", "value"),
        State("pool-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        Input("tabs", "value"),
        State("season-mode-store", "data"),
    )
    def update_player_champions_dropdown_cb(report_id, pool_id, min_friends, tab, is_season_mode):
        if is_season_mode: pool_id = SEASON_POOL_ID
        if tab != "tab-win" or not pool_id: return dash.no_update, dash.no_update
        
        start_date = None
        end_date = None
        if report_id and report_id != "all" and "|" in report_id:
            try:
                start_date, end_date = report_id.split("|")
            except:
                pass
        
        data = render_player_champions_stats(pool_id, queue, min_friends, None, start=start_date, end=end_date)
        if data and "players" in data:
            players = data["players"]
            options = [{"label": p, "value": p} for p in players]
            val = players[0] if players else None
            return options, val
        return [], None

    @app.callback(
        Output("player-champions-graph-container", "children"),
        Input("player-champions-dropdown", "value"),
        Input("report-dropdown", "value"),
        State("pool-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        State("season-mode-store", "data"),
    )
    def update_player_champions_graph_cb(player, report_id, pool_id, min_friends, is_season_mode):
        if is_season_mode: pool_id = SEASON_POOL_ID
        if not player or not pool_id: return html.Div()
        
        start_date = None
        end_date = None
        if report_id and report_id != "all" and "|" in report_id:
            try:
                start_date, end_date = report_id.split("|")
            except:
                pass
        
        output = render_player_champions_stats(pool_id, queue, min_friends, player, start=start_date, end=end_date)
        return dcc.Graph(figure=output["fig"]) if output and "fig" in output else html.Div("No data")

    # --- API & Pipeline Callbacks ---
    @app.callback(
        Output("download-status", "children"),
        Input("btn-download-matches", "n_clicks"),
        prevent_initial_call=True
    )
    def execute_download_cb(n_clicks):
        """Execute L0 only: create user index + download matches"""
        if not n_clicks or n_clicks == 0:
            return dash.no_update
        
        PIPELINE_QUEUE.put("[L0] Iniciando descarga de partidas...\n")
        
        def worker():
            try:
                success = run_l0_only(run_in_terminal=False, queue=PIPELINE_QUEUE)
                if success:
                    PIPELINE_QUEUE.put("[L0] ✅ Descarga completada exitosamente.\n")
                else:
                    PIPELINE_QUEUE.put("[L0] ❌ Error durante la descarga.\n")
            except Exception as e:
                print("[L0 ERROR]", e)
                PIPELINE_QUEUE.put(f"[L0] ❌ Error crítico: {e}\n")
        
        threading.Thread(target=worker).start()
        return html.Span("Descargando partidas...", style={"color": "#00d2ff"})

    @app.callback(
        Output("metrics-generation-status", "children"),
        Input("btn-generate-metrics", "n_clicks"),
        State("pool-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        State("season-mode-store", "data"),
        prevent_initial_call=True
    )
    def execute_metrics_generation_cb(n_clicks, pool_id, min_friends, is_season_mode):
        if is_season_mode: pool_id = SEASON_POOL_ID
        """Execute Standard L1 → L3: filter + flatten + metrics (All Time)"""
        if not n_clicks or n_clicks == 0:
            return dash.no_update
        
        PIPELINE_QUEUE.put(f"[L1-L3] Iniciando actualización global de métricas (Pool: {pool_id}, Min: {min_friends})...\n")
        
        def worker():
            try:
                # Run Full Standard Pipeline
                # No forced dates; L1 creation logic handles season bounds for 'season' pool
                success = run_l1_to_l3(min_friends, pool_id, run_in_terminal=False, queue=PIPELINE_QUEUE)
                if success:
                    PIPELINE_QUEUE.put("[L1-L3] ✅ Actualización completada.\n")
                else:
                    PIPELINE_QUEUE.put("[L1-L3] ❌ Error durante la actualización.\n")
            except Exception as e:
                print("[L1-L3 ERROR]", e)
                PIPELINE_QUEUE.put(f"[L1-L3] ❌ Error crítico: {e}\n")
        
        threading.Thread(target=worker).start()
        return html.Span("Actualizando métricas...", style={"color": "#ffc107"})

    @app.callback(
        Output("report-generation-status", "children"),
        Output("report-trigger-store", "data"),  # Trigger refresh
        Input("btn-generate-report", "n_clicks"),
        State("pool-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        State("date-range-config", "start_date"),
        State("date-range-config", "end_date"),
        State("season-mode-store", "data"),
        prevent_initial_call=True
    )
    def execute_report_generation_cb(n_clicks, pool_id, min_friends, start_date, end_date, is_season_mode):
        if is_season_mode: pool_id = SEASON_POOL_ID
        """Execute Custom Metrics Report for Date Range"""
        if not n_clicks or n_clicks == 0:
             return dash.no_update, dash.no_update
        
        if not start_date or not end_date:
            return html.Span("❌ Por favor, selecciona una fecha de inicio y fin.", style={"color": "red"}), dash.no_update

        PIPELINE_QUEUE.put(f"[REPORT] Iniciando reporte personalizado (Pool: {pool_id}, Dates: {start_date} to {end_date})...\n")
        
        # Store the report ID that will be generated
        new_report_id = f"{start_date}|{end_date}"
        
        def worker():
            try:
                # Run Metrics Script specifically with dates
                res = run_metrics_script(pool_id, queue, min_friends, start_date, end_date)
                if "successfully" in res:
                    PIPELINE_QUEUE.put(f"[REPORT] ✅ Reporte generado: {start_date} a {end_date}\n")
                else:
                    PIPELINE_QUEUE.put(f"[REPORT] ❌ Error generando reporte: {res}\n")
            except Exception as e:
                print("[REPORT ERROR]", e)
                PIPELINE_QUEUE.put(f"[REPORT] ❌ Error crítico: {e}\n")
        
        threading.Thread(target=worker).start()
        
        # Trigger refresh after a delay (store timestamp to trigger update)
        import time
        return html.Span("Generando reporte... (se actualizará automáticamente en 3s)", style={"color": "#9c27b0"}), time.time()

    # Callback to refresh dropdown after report generation
    @app.callback(
        Output("report-dropdown", "options", allow_duplicate=True),
        Output("report-dropdown", "value", allow_duplicate=True),
        Input("report-refresh-interval", "n_intervals"),
        State("pool-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        State("report-trigger-store", "data"),
        prevent_initial_call=True
    )
    def refresh_report_dropdown(n, pool_id, min_friends, trigger_time):
        if not trigger_time or not pool_id:
            return dash.no_update, dash.no_update
        
        # Check if enough time has passed since trigger
        import time
        if time.time() - trigger_time < 3:  # Wait at least 3 seconds
            return dash.no_update, dash.no_update
        
        # Refresh dropdown
        reports = get_available_reports(BASE_DIR, pool_id, queue, min_friends)
        
        # Try to select the most recent report (last in list)
        new_val = reports[-1]["value"] if len(reports) > 1 else "all"
        
        return reports, new_val


    @app.callback(
        Output("api-key-status", "children"),
        Input("btn-save-api", "n_clicks"),
        State("input-api-key", "value"),
        prevent_initial_call=True
    )
    def save_api_key_cb(_, key):
        if not key: return html.Span("Clave inválida", style={"color": "red"})
        try:
            result = save_new_temp_key(key)
            if result["success"]:
                 return html.Span(f"✔ {result['message']} (Válida por 24h+)", style={"color": "#00ff00", "fontWeight": "bold"})
            else:
                 return html.Span(f"❌ {result['message']}", style={"color": "red", "fontWeight": "bold"})
        except Exception as e:
            return html.Span(f"Error: {e}", style={"color": "red"})

    @app.callback(
        Output("current-key-status", "children"),
        Input("tabs", "value"),
        Input("api-key-status", "children")  # Refresh when key is saved
    )
    def show_current_key_status(_, __):
        """Display current API key status"""
        import os
        from utils.api_key_manager import get_api_key
        
        env_key = os.getenv("RIOT_API_KEY")
        
        try:
            key = get_api_key()
            masked = f"***{key[-6:]}"
            
            if env_key and env_key == key:
                return html.Div([
                    html.Span("⚠️ ", style={"color": "orange"}),
                    html.Span(f"Usando clave de .env: {masked}", style={"color": "#ff9800", "fontSize": "0.9em"}),
                    html.Br(),
                    html.Span("(Las claves guardadas desde la web serán ignoradas)", style={"color": "#888", "fontSize": "0.8em"})
                ])
            else:
                return html.Div([
                    html.Span("✅ ", style={"color": "lime"}),
                    html.Span(f"Clave Activa: {masked}", style={"color": "#888", "fontSize": "0.9em"})
                ])
        except:
            return html.Div([
                html.Span("⚠️ ", style={"color": "orange"}),
                html.Span("No se encontró una clave API válida", style={"color": "#ff9800", "fontSize": "0.9em"})
            ])

    @app.callback(
        Output("pipeline-status", "children"),
        Output("pipeline-status-store", "data"),
        Input("btn-run-pipeline", "n_clicks"),
        State("pool-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        State("season-mode-store", "data"),
        prevent_initial_call=True
    )
    def execute_pipeline_cb(n_clicks, pool_id, min_friends, is_season_mode):
        if is_season_mode: pool_id = SEASON_POOL_ID
        # Safety check: only run if button was actually clicked
        if not n_clicks or n_clicks == 0:
            return dash.no_update, dash.no_update
        
        # Immediate feedback
        PIPELINE_QUEUE.put(f"[PIPELINE] Iniciando pipeline completo (Pool: {pool_id}, Min Friends: {min_friends})...\n")
            
        def worker():
            try:
                run_full_pipeline(min_friends, pool_id, run_in_terminal=False, queue=PIPELINE_QUEUE)
            except Exception as e:
                print("[PIPELINE ERROR]", e)
                PIPELINE_QUEUE.put(f"[PIPELINE] Error crítico en pipeline principal: {e}\n")
                
                # Fallback: Ejecutar solo el script de consultar partidas
                PIPELINE_QUEUE.put("[PIPELINE] Iniciando fallback: ejecutando script de consultar partidas...\n")
                try:
                    fallback_script = BASE_DIR / "src" / "extract" / "0_getAllMatchesFromAPI.py"
                    cmd = [sys.executable, str(fallback_script)]
                    
                    print("[FALLBACK] Executing:", " ".join(cmd))
                    PIPELINE_QUEUE.put(f"[FALLBACK] Comando: {' '.join(cmd)}\n")
                    
                    # Use Popen for streaming output
                    process = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True,
                        encoding="utf-8",
                        errors="replace"
                    )
                    
                    # Stream reader for fallback
                    def stream_fallback_output(stream, prefix="[FALLBACK] "):
                        for line in iter(stream.readline, ""):
                            PIPELINE_QUEUE.put(f"{prefix}{line}")
                        stream.close()
                    
                    # Start threads to read stdout and stderr
                    t_out = threading.Thread(target=stream_fallback_output, args=(process.stdout,))
                    t_err = threading.Thread(target=stream_fallback_output, args=(process.stderr,))
                    
                    t_out.start()
                    t_err.start()
                    
                    t_out.join()
                    t_err.join()
                    
                    process.wait()
                    
                    if process.returncode == 0:
                        PIPELINE_QUEUE.put("[FALLBACK] ✅ Script de consultar partidas completado exitosamente.\n")
                    else:
                        PIPELINE_QUEUE.put(f"[FALLBACK] ❌ Script de consultar partidas falló con código {process.returncode}.\n")
                        
                except KeyboardInterrupt:
                    PIPELINE_QUEUE.put("[FALLBACK] ⚠️ Script de consultar partidas CANCELADO por el usuario.\n")
                    print("[FALLBACK] Cancelled by user")
                except Exception as fallback_error:
                    PIPELINE_QUEUE.put(f"[FALLBACK] ❌ Error ejecutando script de consultar partidas: {fallback_error}\n")
                    print("[FALLBACK ERROR]", fallback_error)

        threading.Thread(target=worker).start()
        return html.Span("Pipeline Iniciado...", style={"color": "#00d2ff"}), "RUNNING"

    @app.callback(
        Output("pipeline-log", "children"),
        Input("log-refresh", "n_intervals"),
        State("pipeline-log", "children"),
        prevent_initial_call=False
    )
    def update_log_cb(_, current_log):
        lines = []
        while not PIPELINE_QUEUE.empty():
            lines.append(PIPELINE_QUEUE.get())
        if not lines: return dash.no_update
        
        current_text = current_log if isinstance(current_log, str) else ""
        new_log = str(current_text) + "".join(lines)
        
        # Basic check for error in lines
        # This is a bit hacky, normally we'd update the Store via this callback too, 
        # but Output("pipeline-status-store", "data") is already mapped above.
        # Dash allows multiple outputs if careful, but logic separation is better.
        # For this task, we will just rely on the logs visible for details, 
        # but requested "Notificar en el front".
        # Let's check the store via a clientside callback or separate callback monitoring log changes?
        # Simpler: Make THIS callback also output to an Alert div if it detects keywords.
        
        return new_log

    @app.callback(
        Output("pipeline-alert", "children"),
        Input("pipeline-log", "children"),
    )
    def check_pipeline_error(log_text):
        if not log_text: return dash.no_update
        
        # Check for fallback cancellation
        if "[FALLBACK] ⚠️ Script de consultar partidas CANCELADO" in log_text:
            return html.Div([
                html.H4("⚠️ Fallback Cancelado"),
                html.P("El pipeline principal falló y el script de consultar partidas fue cancelado."),
                html.P("Revisa los logs para más detalles.", style={"fontSize": "0.9em", "marginTop": "5px"})
            ], style={"backgroundColor": "#664400", "color": "#ffcc00", "padding": "10px", "borderRadius": "5px", "border": "1px solid #ffaa00"})
        
        # Check for fallback failure
        if "[FALLBACK] ❌ Script de consultar partidas falló" in log_text or "[FALLBACK] ❌ Error ejecutando" in log_text:
            return html.Div([
                html.H4("❌ Pipeline y Fallback Fallaron"),
                html.P("El pipeline principal falló y el script de consultar partidas también falló."),
                html.P("Revisa los logs para más detalles.", style={"fontSize": "0.9em", "marginTop": "5px"})
            ], style={"backgroundColor": "#440000", "color": "red", "padding": "10px", "borderRadius": "5px", "border": "1px solid red"})
        
        # Check for fallback success
        if "[FALLBACK] ✅ Script de consultar partidas completado exitosamente" in log_text:
            return html.Div([
                html.H4("⚠️ Pipeline Parcialmente Completado"),
                html.P("El pipeline principal falló, pero el script de consultar partidas se ejecutó exitosamente."),
                html.P("Las partidas fueron descargadas, pero es posible que falten métricas procesadas.", style={"fontSize": "0.9em", "marginTop": "5px"})
            ], style={"backgroundColor": "#443300", "color": "#ffdd88", "padding": "10px", "borderRadius": "5px", "border": "1px solid #ff9900"})
        
        # Check for main pipeline error (without fallback completion yet)
        if "[PIPELINE] Error" in log_text or "Abortando" in log_text:
            # Only show this if we haven't seen fallback messages yet
            if "[FALLBACK]" not in log_text:
                return html.Div([
                    html.H4("❌ Pipeline Falló"),
                    html.P("Ejecutando fallback: script de consultar partidas..."),
                    html.P("Espera a que complete la descarga de partidas.", style={"fontSize": "0.9em", "marginTop": "5px"})
                ], style={"backgroundColor": "#440000", "color": "red", "padding": "10px", "borderRadius": "5px", "border": "1px solid red"})
        
        # Check for full pipeline success
        if "[PIPELINE] Finalizado L3" in log_text: # Last step success check
             return html.Div("✔ Pipeline Completado Exitosamente", style={"color": "lime", "fontWeight": "bold"})
        
        return dash.no_update

    # --- Cross-Role Comparison Callbacks ---
    @app.callback(
        Output("cross-role-player1", "options"),
        Output("cross-role-player2", "options"),
        Input("pool-dropdown", "value"),
        Input("min-friends-dropdown", "value"),
        State("season-mode-store", "data"),
    )
    def update_cross_role_players(pool_id, min_friends, is_season_mode):
        """Populate player dropdowns for cross-role comparison"""
        if is_season_mode:
            pool_id = SEASON_POOL_ID
        if not pool_id:
            return [], []
        
        try:
            from charts.chart_10_metrics_stats_by_rol import get_data_file, load_json
            data_file = get_data_file(pool_id, queue, min_friends)
            raw = load_json(data_file)
            
            if not raw or "roles" not in raw:
                return [], []
            
            # Get all unique players across all roles
            all_players = set()
            for role_data in raw["roles"].values():
                all_players.update(role_data.keys())
            
            options = [{"label": p, "value": p} for p in sorted(all_players)]
            return options, options
        except:
            return [], []
    
    @app.callback(
        Output("cross-role-graph-container", "children"),
        Input("btn-generate-cross-role", "n_clicks"),
        State("cross-role-player1", "value"),
        State("cross-role-role1", "value"),
        State("cross-role-player2", "value"),
        State("cross-role-role2", "value"),
        State("pool-dropdown", "value"),
        State("min-friends-dropdown", "value"),
        State("report-dropdown", "value"),
        State("season-mode-store", "data"),
        prevent_initial_call=True
    )
    def generate_cross_role_graph(n_clicks, p1, r1, p2, r2, pool_id, min_friends, report_id, is_season_mode):
        """Generate cross-role comparison radar chart"""
        if is_season_mode:
            pool_id = SEASON_POOL_ID
        
        if not pool_id or not p1 or not r1:
            return html.Div("Selecciona al menos un jugador y su rol", style={"color": "#888", "textAlign": "center", "padding": "20px"})
        
        # Parse dates if applicable
        start_date = None
        end_date = None
        if report_id and report_id != "all" and "|" in report_id:
            try:
                start_date, end_date = report_id.split("|")
            except:
                pass
        
        # Build selections
        selections = [(p1, r1)]
        if p2 and r2:
            selections.append((p2, r2))
        
        try:
            from charts.chart_10_metrics_stats_by_rol import render_cross_role
            
            figs = render_cross_role(
                pool_id=pool_id,
                queue=queue,
                min_friends=min_friends,
                player_role_selections=selections,
                start=start_date,
                end=end_date
            )
            
            if figs and len(figs) > 0:
                return html.Div(
                    dcc.Graph(figure=figs[0]),
                    className="graph-card"
                )
            else:
                return html.Div("No se pudieron cargar datos", style={"color": "red", "textAlign": "center"})
        except Exception as e:
            return html.Div(f"Error: {str(e)}", style={"color": "red", "textAlign": "center"})

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host=HOST, port=PORT, debug=False)
