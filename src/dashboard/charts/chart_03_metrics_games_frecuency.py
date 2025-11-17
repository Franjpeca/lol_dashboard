import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.io as pio
from dash import html, dcc

pio.templates.default = "plotly_dark"

BASE_DIR = Path(__file__).resolve().parents[3]

# ============================================================
# LOCALIZAR RUTAS SEGÚN pool / queue / min
# ============================================================

def get_paths(pool_id, queue, min_friends):
    global_file = BASE_DIR / f"data/results/pool_{pool_id}/q{queue}/min{min_friends}/metrics_03_games_frecuency.json"
    return global_file

# ============================================================
# Helpers
# ============================================================

def load_json(path: Path):
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

def build_df_personas(data):
    # Procesar los datos de 'players' o 'users' dependiendo del formato del JSON
    if 'players' in data:
        return pd.DataFrame(
            [
                {
                    "persona": u["persona"],
                    "winrate": round(u["winrate"] * 100, 2),
                    "games": u["total_games"],
                }
                for u in data["players"]
            ]
        )
    else:
        print(f"[WARN] 'players' key not found in data")
        return pd.DataFrame()

def build_df_cuentas(data):
    # Procesar las cuentas de cada jugador
    rows = []
    if 'players' in data:
        for player_info in data["players"]:
            persona = player_info["persona"]
            for puuid, acc in player_info["puuids"].items():
                rows.append(
                    {
                        "persona": persona,
                        "riotId": acc["riotId"],
                        "puuid": puuid,
                        "winrate": round(acc["winrate"] * 100, 2),
                        "games": acc["games"],
                    }
                )
    else:
        print(f"[WARN] 'players' key not found in data")
    return pd.DataFrame(rows)

def expand_dates(df: pd.DataFrame, min_date: str, max_date: str):
    full_range = pd.date_range(start=min_date, end=max_date, freq='D')
    base = pd.DataFrame({"date": full_range})

    if df.empty:
        base["games"] = 0
        return base

    df["date"] = pd.to_datetime(df["date"])
    merged = base.merge(df, on="date", how="left")
    merged["games"] = merged["games"].fillna(0)
    merged["date"] = merged["date"].dt.strftime("%Y-%m-%d")
    return merged

# ============================================================
# Figures
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

# ============================================================
# FUNCION PRINCIPAL PARA DASH
# ============================================================

def render(pool_id: str, queue: int, min_friends: int):
    # Usa los parámetros pool_id, queue y min_friends para obtener el archivo de datos
    data_path = get_paths(pool_id, queue, min_friends)

    if not data_path.exists():
        print(f"[WARN] champions file not found: {data_path}")
        return []

    data = load_json(data_path)
    df_persona = build_df_personas(data)
    df_cuentas = build_df_cuentas(data)

    figs = []

    if not df_persona.empty:
        figs.append(make_global_fig(df_persona))
        figs.append(make_player_fig(df_persona, "Jugador"))

    return figs
