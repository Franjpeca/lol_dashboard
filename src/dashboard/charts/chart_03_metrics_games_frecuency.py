import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.io as pio
from dash import html, dcc

pio.templates.default = "plotly_dark"

BASE_DIR = Path(__file__).resolve().parents[3]


def get_paths(pool_id, queue, min_friends):
    return BASE_DIR / f"data/results/pool_{pool_id}/q{queue}/min{min_friends}/metrics_03_games_frecuency.json"


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


def make_global_fig(df):
    fig = px.bar(
        df,
        x="date",
        y="games",
        text="games",
        color="games",
        color_continuous_scale="Turbo",
        labels={"games": "Partidas", "date": "Fecha"},
        title="Frecuencia global de partidas por día",
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
        title=f"Partidas por día - {persona}",
    )

    fig.update_traces(textposition="outside")

    fig.update_layout(
        autosize=True,
        height=450,
        margin=dict(l=20, r=20, t=60, b=40),
        xaxis=dict(type="category"),
    )
    return fig


def render(pool_id: str, queue: int, min_friends: int):
    print("[INFO] Loading chart_03_metrics_games_frecuency.py")

    data_path = get_paths(pool_id, queue, min_friends)
    data = load_json(data_path)

    df_global = build_df_global(data)
    df_players = build_df_players(data)

    figs = []

    if not df_global.empty:
        figs.append(make_global_fig(df_global))

    for persona, df in df_players.items():
        if not df.empty:
            figs.append(make_player_fig(df, persona))

    print("[DEBUG] RESULTADO FREQUENCY:", len(figs), "figuras generadas")

    return figs
