import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.io as pio
from dash import Dash, html, dcc, callback, Output, Input

pio.templates.default = "plotly_dark"

BASE_DIR = Path(__file__).resolve().parents[3]


# ============================================================
#   LOCALIZAR ARCHIVO SEGÚN pool / queue / min / FECHAS
# ============================================================

def get_data_file(pool_id: str, queue: int, min_friends: int, start_date=None, end_date=None) -> Path:
    """
    Obtiene la ruta del archivo de datos basado en los parámetros proporcionados.
    Si se proporcionan fechas, buscará el archivo con esas fechas en el nombre.
    """
    if start_date and end_date:
        return BASE_DIR / f"data/runtime/pool_{pool_id}/q{queue}/min{min_friends}/metrics_04_win_lose_streak_{start_date}_to_{end_date}.json"
    else:
        return BASE_DIR / f"data/results/pool_{pool_id}/q{queue}/min{min_friends}/metrics_04_win_lose_streak.json"


# ============================================================
#   CARGA DE DATOS
# ============================================================

def load_json(path: Path):
    """
    Carga los datos JSON desde la ruta proporcionada.
    """
    if not path.exists():
        print(f"[ERROR] Archivo no encontrado: {path}")
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_df_streaks(raw_data):
    """
    Construye el DataFrame para las rachas de victorias y derrotas.
    El JSON nuevo contiene la estructura:
    {
        "source_L1": "...",
        "start_date": "...",
        "end_date": "...",
        "generated_at": "...",
        "streaks": {
            "Frieren": {...},
            "Kyoraku": {...}
        }
    }
    """
    if not raw_data or "streaks" not in raw_data:
        return pd.DataFrame()

    data = raw_data["streaks"]

    rows = []
    for persona, stats in data.items():
        rows.append({
            "persona": persona,
            "max_win_streak": stats.get("max_win_streak", 0),
            "max_lose_streak": stats.get("max_lose_streak", 0),
            "current_streak": stats.get("current_streak", 0),
        })

    return pd.DataFrame(rows)


# ============================================================
#   FUNCIONES PARA LAS GRÁFICAS
# ============================================================

def make_fig(df: pd.DataFrame, value_col: str, title: str):
    """
    Genera una figura genérica con el estilo del script original.
    """
    n = len(df)
    tick_font = max(12, min(22, int(320 / max(1, n))))
    height = int(max(550, min(1000, 35 * max(1, n))) * 0.75)

    fig = px.bar(
        df,
        x=value_col,
        y="persona",
        orientation="h",
        text=value_col,
        color=value_col,
        color_continuous_scale="Turbo",
        title=title,
    )

    fig.update_traces(
        textposition="inside",
        insidetextanchor="middle",
        marker_line_width=0,
        textfont=dict(color="white", size=max(12, min(20, int(220 / max(1, n))))),
    )

    fig.update_layout(
        bargap=0.18,
        autosize=True,
        height=height + 90,
        margin=dict(l=170, r=50, t=60, b=40),
        xaxis_title="",
        yaxis=dict(
            type="category",
            tickvals=df["persona"].tolist(),
            ticktext=df["persona"].tolist(),
            tickfont=dict(size=tick_font),
            automargin=True,
        ),
    )

    return fig


def make_win_streak_fig(df):
    """
    Genera la figura de rachas máximas de victorias.
    """
    df_win = df[["persona", "max_win_streak"]].copy()
    df_win = df_win.sort_values("max_win_streak")
    return make_fig(df_win, "max_win_streak", "Mayor racha de victorias por persona")


def make_lose_streak_fig(df):
    """
    Genera la figura de rachas máximas de derrotas.
    """
    df_lose = df[["persona", "max_lose_streak"]].copy()
    df_lose = df_lose.sort_values("max_lose_streak")
    return make_fig(df_lose, "max_lose_streak", "Mayor racha de derrotas por persona")


# ============================================================
#   FUNCION PRINCIPAL PARA DASH
# ============================================================

def create_app(pool_id="ac89fa8d", queue=440):
    """
    Crea y configura la aplicación de Dash.
    """
    app = Dash(__name__)

    # Definir la estructura de la página
    app.layout = html.Div([
        html.H1("🎮 Rachas de Jugadores", 
                style={"textAlign": "center", "marginBottom": "30px"}),

        html.Div([
            html.Label("Selecciona mínimo de amigos:", 
                      style={"fontWeight": "bold", "marginBottom": "10px"}),
            dcc.Dropdown(
                id="dropdown-min-friends", 
                options=[
                    {"label": "4 amigos mínimo", "value": 4},
                    {"label": "5 amigos mínimo", "value": 5}
                ], 
                value=5, 
                style={"width": "100%"}
            ),
        ], style={"width": "30%", "margin": "auto", "marginBottom": "30px"}),

        html.Div([ 
            html.Label("Fecha de inicio:", 
                       style={"fontWeight": "bold", "marginBottom": "10px"}),
            dcc.DatePickerSingle(
                id="start-date-picker",
                display_format="YYYY-MM-DD",
                style={"width": "100%"},
            ),
        ], style={"width": "30%", "margin": "auto", "marginBottom": "30px"}),

        html.Div([ 
            html.Label("Fecha de fin:", 
                       style={"fontWeight": "bold", "marginBottom": "10px"}),
            dcc.DatePickerSingle(
                id="end-date-picker",
                display_format="YYYY-MM-DD",
                style={"width": "100%"},
            ),
        ], style={"width": "30%", "margin": "auto", "marginBottom": "30px"}),

        html.Div(id="graphs-container")
    ])

    @callback(
        Output("graphs-container", "children"),
        [
            Input("dropdown-min-friends", "value"),
            Input("start-date-picker", "date"),
            Input("end-date-picker", "date")
        ]
    )
    def update_graphs(min_friends, start_date, end_date):
        """
        Actualiza las gráficas cuando se selecciona el número de amigos o las fechas.
        """
        data_file = get_data_file(pool_id, queue, min_friends, start_date, end_date)

        # Cargar los datos
        data = load_json(data_file)

        if not data:
            return html.Div([
                html.H3("⚠️ No se encontraron datos", 
                       style={"textAlign": "center", "color": "#ef4444"})
            ])

        df_streaks = build_df_streaks(data)

        if df_streaks.empty:
            return html.Div([
                html.H3("⚠️ No hay datos disponibles", 
                       style={"textAlign": "center", "color": "#ef4444"})
            ])

        # Crear las gráficas
        return html.Div([
            dcc.Graph(figure=make_win_streak_fig(df_streaks)),
            html.Hr(),
            dcc.Graph(figure=make_lose_streak_fig(df_streaks)),
        ])

    return app


# ============================================================
#   FUNCIÓN PARA USAR EN TU FLUJO (sin Dash)
# ============================================================

def render(pool_id: str, queue: int, min_friends: int, start=None, end=None):
    """
    Función que retorna las figuras de Plotly para usar en tu flujo existente.
    """
    data_file = get_data_file(pool_id, queue, min_friends, start, end)
    raw_data = load_json(data_file)

    if not raw_data:
        print(f"[ERROR] No se pudieron cargar datos de {data_file}")
        return []

    df_streaks = build_df_streaks(raw_data)

    if df_streaks.empty:
        print("[WARN] DataFrame de rachas está vacío")
        return []

    return [
        make_win_streak_fig(df_streaks),
        make_lose_streak_fig(df_streaks),
    ]


# ============================================================
#   ENTRY POINT
# ============================================================

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
