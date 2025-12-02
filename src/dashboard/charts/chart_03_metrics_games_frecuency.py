import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.io as pio
from dash import html, dcc

pio.templates.default = "plotly_dark"

BASE_DIR = Path(__file__).resolve().parents[3]


# ============================================================
#   LOCALIZAR RUTA DEL JSON SEGUN POOL / QUEUE / MIN / FECHAS
# ============================================================

def get_paths(pool_id, queue, min_friends, start_date=None, end_date=None):
    # Si se proporcionan fechas, busca el archivo con el rango de fechas en el nombre
    if start_date and end_date:
        return BASE_DIR / f"data/runtime/pool_{pool_id}/q{queue}/min{min_friends}/metrics_03_games_frecuency_{start_date}_to_{end_date}.json"
    else:
        # Si no se proporcionan fechas, busca el archivo predeterminado
        return BASE_DIR / f"data/results/pool_{pool_id}/q{queue}/min{min_friends}/metrics_03_games_frecuency.json"


# ============================================================
#   CARGA
# ============================================================

def load_json(path: Path):
    if not path.exists():
        print(f"[WARN] No existe el archivo: {path}")
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_df_global(data):
    if "global_frequency" not in data:
        print("[WARN] 'global_frequency' missing in JSON")
        return pd.DataFrame()
    return pd.DataFrame(data["global_frequency"])


def build_df_players(data):
    players = {}
    players_list = data.get("players_frequency", [])
    if not isinstance(players_list, list):
        print("[WARN] 'players_frequency' no es una lista")
        return players

    for entry in players_list:
        persona = entry.get("persona", "Unknown")
        games_list = entry.get("games", [])
        df_games = pd.DataFrame(games_list)
        if df_games.empty:
            continue

        if persona in players:
            players[persona] = pd.concat([players[persona], df_games], ignore_index=True)
        else:
            players[persona] = df_games

    return players


# ============================================================
#   FIGURA HORIZONTAL
# ============================================================

def make_fig_horizontal(df: pd.DataFrame, x: str, title: str):

    n = len(df)

    tick_font_size = max(12, min(22, int(320 / n)))

    base_height = max(750, min(1200, 35 * n))
    fig_height = int(base_height * 0.75)

    fig = px.bar(
        df,
        x=x,
        y="date",
        orientation="h",
        text=x,
        color="games",
        color_continuous_scale="Turbo",
        labels={"games": "Partidas", "date": "Fecha"},
        hover_data=["games"],
        title=title,
    )

    fig.update_layout(bargap=0.18)

    fig.update_traces(
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(
            color="white",
            size=max(12, min(20, int(220 / n))),
        ),
        marker_line_width=0,
    )

    fig.update_layout(
        autosize=True,
        height=fig_height + 90,
        margin=dict(l=170, r=50, t=60, b=40),
        xaxis_title="",
        yaxis=dict(
            type="category",
            tickmode="array",
            tickvals=df["date"].tolist(),
            ticktext=df["date"].tolist(),
            tickfont=dict(size=tick_font_size),
            automargin=True,
        ),
    )

    return fig


# ============================================================
#   RENDER PRINCIPAL PARA DASH
# ============================================================

def render(pool_id: str, queue: int, min_friends: int, start=None, end=None):
    print("[INFO] Loading chart_03_metrics_games_frecuency.py")

    # Llamada a get_paths con las fechas
    data_path = get_paths(pool_id, queue, min_friends, start, end)
    data = load_json(data_path)

    df_global = build_df_global(data)
    df_players = build_df_players(data)

    result = {
        "global": None,
        "players": {}
    }

    if not df_global.empty:
        result["global"] = make_fig_horizontal(df_global, "games", "Frecuencia global de partidas por día")

    for persona in sorted(df_players.keys()):
        df = df_players[persona]
        if not df.empty:
            result["players"][persona] = make_fig_horizontal(df, "games", f"Frecuencia de partidas por día - {persona}")

    total_figs = (1 if result["global"] is not None else 0) + len(result["players"])
    print("[DEBUG] RESULTADO FREQUENCY:", total_figs, "figuras generadas")

    return result
