import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.io as pio
from dash import html, dcc

pio.templates.default = "plotly_dark"

BASE_DIR = Path(__file__).resolve().parents[3]

# ============================================================
#   LOCALIZAR FICHERO SEGÚN pool_id / queue / min / start_date / end_date
# ============================================================

def get_data_file(pool_id: str, queue: int, min_friends: int, start_date=None, end_date=None) -> Path:
    if start_date and end_date:
        return (
            BASE_DIR
            / "data"
            / "runtime"
            / f"pool_{pool_id}"
            / f"q{queue}"
            / f"min{min_friends}"
            / f"metrics_01_players_games_winrate_{start_date}_to_{end_date}.json"
        )
    else:
        return (
            BASE_DIR
            / "data"
            / "results"
            / f"pool_{pool_id}"
            / f"q{queue}"
            / f"min{min_friends}"
            / "metrics_01_players_games_winrate.json"
        )


# ============================================================
#   CARGA
# ============================================================

def load_data(file_path: Path):
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_df_personas(data):
    return pd.DataFrame(
        [
            {
                "persona": u["persona"],
                "winrate": round(u["winrate"] * 100, 2),
                "games": u["total_games"],
            }
            for u in data["users"]
        ]
    )


def build_df_cuentas(data):
    rows = []
    for user in data["users"]:
        persona = user["persona"]
        for puuid, acc in user["puuids"].items():
            rows.append(
                {
                    "persona": persona,
                    "riotId": acc["riotId"],
                    "puuid": puuid,
                    "winrate": round(acc["winrate"] * 100, 2),
                    "games": acc["games"],
                }
            )
    return pd.DataFrame(rows)


# ==========================
#   FIGURAS
# ==========================

def make_fig_winrate(df: pd.DataFrame, x: str, title: str):
    fig = px.bar(
        df,
        x=x,
        y="winrate",
        text="winrate",
        color="games",  # color = número de partidas
        color_continuous_scale="Viridis",
        labels={"winrate": "Winrate (%)", "games": "Partidas"},
        hover_data=["games"],
        title=title,
    )
    fig.update_traces(
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(color="white", size=12),
    )
    fig.update_layout(
        autosize=True,
        margin=dict(l=20, r=20, t=60, b=60),
        xaxis_title="",
        yaxis_title="Winrate (%)",
    )
    return fig


def make_fig_games(df: pd.DataFrame, x: str, title: str):
    fig = px.bar(
        df,
        x=x,
        y="games",
        text="games",
        color="games",  # color = partidas
        color_continuous_scale="Turbo",
        labels={"games": "Partidas", "winrate": "Winrate (%)"},
        hover_data=["winrate"],
        title=title,
    )
    fig.update_traces(
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(color="white", size=12),
    )
    fig.update_layout(
        autosize=True,
        margin=dict(l=20, r=20, t=60, b=60),
        xaxis_title="",
        yaxis_title="Partidas",
    )
    return fig


# ==========================
#   FUNCION PRINCIPAL PARA DASH
# ==========================

def render(pool_id: str, queue: int, min_friends: int, start=None, end=None):
    # Usa los parámetros pool_id, queue y min_friends para obtener el archivo de datos
    data_path = get_data_file(pool_id, queue, min_friends, start, end)

    if not data_path.exists():
        print(f"[WARN] champions file not found: {data_path}")
        return []

    data = load_data(data_path)
    df_persona = build_df_personas(data)
    df_cuentas = build_df_cuentas(data)
    global_mean = round(data["global_winrate_mean"] * 100, 2)

    figs = []

    if not df_persona.empty:
        figs.append(make_fig_winrate(df_persona, "persona", "Winrate por persona (%)"))
        figs.append(make_fig_games(df_persona, "persona", "Partidas por persona"))

    if not df_cuentas.empty:
        figs.append(make_fig_winrate(df_cuentas, "riotId", "Winrate por cuenta (%)"))
        figs.append(make_fig_games(df_cuentas, "riotId", "Partidas por cuenta"))

    return figs
