import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.io as pio

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
            / f"metrics_12_botlane_synergy_{start_date}_to_{end_date}.json"
        )
    else:
        return (
            BASE_DIR
            / "data"
            / "results"
            / f"pool_{pool_id}"
            / f"q{queue}"
            / f"min{min_friends}"
            / "metrics_12_botlane_synergy.json"
        )


# ============================================================
#   CARGA
# ============================================================

def load_data(file_path: Path):
    with file_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def build_df_synergy(data):
    df = pd.DataFrame(data["botlane_synergy"])
    df["duo_str"] = df["duo"].apply(lambda x: " y ".join(x))
    df["botlane_score"] = round(df["botlane_score"] * 100, 2)
    return df


# ==========================
#   FIGURAS
# ==========================

def make_fig_synergy(df: pd.DataFrame, title: str):
    fig = px.bar(
        df,
        x="duo_str",
        y="botlane_score",
        text="botlane_score",
        color="games",
        color_continuous_scale="Viridis",
        labels={"botlane_score": "Synergy Score", "games": "Partidas", "duo_str": "Duo"},
        hover_data=["winrate", "avg_kda"],
        title=title,
    )
    fig.update_traces(
        textposition="outside",
        textfont=dict(color="white", size=12),
    )
    fig.update_layout(
        autosize=True,
        margin=dict(l=20, r=20, t=60, b=20),
        xaxis_title="",
        yaxis_title="Synergy Score",
        yaxis=dict(range=[0, df['botlane_score'].max() * 1.15])
    )
    return fig

def make_fig_player_synergy(df: pd.DataFrame, player: str):
    df_player = df[df["duo"].apply(lambda d: player in d)].copy()
    if df_player.empty:
        return None

    df_player["partner"] = df_player["duo"].apply(lambda d: d[0] if d[1] == player else d[1])
    df_player = df_player.sort_values(by="botlane_score", ascending=False)

    fig = px.bar(
        df_player,
        x="partner",
        y="botlane_score",
        text="botlane_score",
        color="games",
        color_continuous_scale="Plasma",
        labels={"botlane_score": "Synergy Score", "games": "Partidas", "partner": "Compañero"},
        hover_data=["winrate", "avg_kda"],
        title=f"Mejores compañeros de botlane para {player}",
    )
    fig.update_traces(textposition="outside", textfont=dict(color="white", size=12))
    fig.update_layout(autosize=True, margin=dict(l=20, r=20, t=60, b=20), xaxis_title="", yaxis_title="Synergy Score")
    fig.update_yaxes(range=[0, df_player['botlane_score'].max() * 1.15])
    return fig

# ==========================
#   FUNCION PRINCIPAL PARA DASH
# ==========================

def render(pool_id: str, queue: int, min_friends: int, min_games: int = 0, start=None, end=None):
    data_path = get_data_file(pool_id, queue, min_friends, start, end)
    if not data_path.exists():
        print(f"[WARN] botlane synergy file not found: {data_path}")
        return []

    data = load_data(data_path)
    df = build_df_synergy(data)

    if min_games > 0:
        df = df[df["games"] >= min_games]

    if df.empty:
        return []

    df_top10 = df.head(10)
    df_worst10 = df.tail(10).sort_values(by="botlane_score", ascending=True)

    figs = []
    if not df_top10.empty:
        figs.append(make_fig_synergy(df_top10, "Top 10 Sinergia en Botlane"))
    if not df_worst10.empty:
        figs.append(make_fig_synergy(df_worst10, "Peor 10 Sinergia en Botlane"))

    return figs


def get_synergy_data(pool_id: str, queue: int, min_friends: int, start=None, end=None):
    """Carga los datos de sinergia y devuelve el DataFrame y la lista de jugadores."""
    data_path = get_data_file(pool_id, queue, min_friends, start, end)
    if not data_path.exists():
        return None, []

    data = load_data(data_path)
    df = build_df_synergy(data)

    if df.empty:
        return None, []

    # Obtener una lista única de todos los jugadores en los dúos
    all_players = sorted(list(set(p for duo in df["duo"] for p in duo)))

    return df, all_players