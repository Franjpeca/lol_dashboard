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

def make_global_fig(df):
    fig = px.bar(
        df,
        x="date",
        y="games",
        text="games",
        color="games",
        color_continuous_scale="Turbo",
        labels={"games": "Partidas", "date": "Fecha"},
        title="Frecuencia global de partidas por dia",
    )

    fig.update_traces(textposition="outside")

    fig.update_layout(
        autosize=True,
        height=450,
        margin=dict(l=20, r=20, t=60, b=40),
        xaxis=dict(type="category"),
    )
    return fig


def make_player_fig(df, persona):
    fig = px.bar(
        df,
        x="date",
        y="games",
        text="games",
        color="games",
        color_continuous_scale="Tealgrn",
        labels={"games": "Partidas", "date": "Fecha"},
        title=f"Partidas por dia - {persona}",
    )

    fig.update_traces(textposition="outside")

    fig.update_layout(
        autosize=True,
        height=450,
        margin=dict(l=20, r=20, t=60, b=40),
        xaxis=dict(type="category"),
    )
    return fig


# ============================================================
#   RENDER PRINCIPAL PARA DASH
# ============================================================

def render(pool_id: str, queue: int, min_friends: int, start=None, end=None):
    # print("[INFO] Loading chart_03_metrics_games_frecuency.py") # Comentado para reducir logs

    # Llamada a get_paths con las fechas
    data_path = get_paths(pool_id, queue, min_friends, start, end)
    data = load_json(data_path)

    df_global = build_df_global(data)
    df_players = build_df_players(data)

    result = {
        "global": None,
        "players": {},
    }

    if not df_global.empty:
        result["global"] = make_global_fig(df_global)

    for persona in sorted(df_players.keys()):
        df = df_players[persona]
        if not df.empty:
            result["players"][persona] = make_player_fig(df, persona)

    return result
