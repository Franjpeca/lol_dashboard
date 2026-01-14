import json
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
import plotly.express as px
import plotly.io as pio

pio.templates.default = "plotly_dark"

BASE_DIR = Path(__file__).resolve().parents[3]

def get_data_file(pool_id: str, queue: int, min_friends: int, start_date=None, end_date=None) -> Path:
    if start_date and end_date:
        return (
            BASE_DIR / "data" / "runtime" / f"pool_{pool_id}"
            / f"q{queue}" / f"min{min_friends}"
            / f"metrics_13_player_champions_stats_{start_date}_to_{end_date}.json"
        )
    return (
        BASE_DIR / "data" / "results" / f"pool_{pool_id}"
        / f"q{queue}" / f"min{min_friends}"
        / "metrics_13_player_champions_stats.json"
    )

def load_data(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

# ============================
# FIGURA HORIZONTAL (estilo chart_02)
# ============================

def make_fig_horizontal(df: pd.DataFrame, title: str, player: str):

    n = len(df)
    tick_font_size = max(12, min(22, int(320 / n)))

    base_height = max(750, min(1200, 35 * n))
    fig_height = int(base_height * 0.75)

    fig = px.bar(
        df,
        x="winrate",
        y="champion",
        orientation="h",
        text="winrate",
        color="games",
        color_continuous_scale="Turbo",
        labels={"games": "Partidas", "winrate": "Winrate (%)"},
        hover_data=["games", "winrate"],
        title=title,
    )

    fig.update_traces(
        texttemplate="%{x:.0f}%",
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(color="white", size=max(12, min(20, int(220 / n)))),
        marker_line_width=0,
    )

    fig.update_layout(
        autosize=True,
        height=fig_height + 90,
        bargap=0.18,
        margin=dict(l=170, r=50, t=60, b=40),
        xaxis_title="",
        xaxis_range=[0, 105],
        yaxis=dict(
            type="category",
            tickmode="array",
            tickvals=df["champion"].tolist(),
            ticktext=df["champion"].tolist(),
            tickfont=dict(size=tick_font_size),
            automargin=True,
        ),
    )

    return fig


# ============================
# RENDER PRINCIPAL (para DASH)
# ============================

def render(pool_id: str, queue: int, min_friends: int, player: Optional[str],
           start: Optional[str] = None, end: Optional[str] = None) -> Optional[Dict[str, Any]]:

    data_path = get_data_file(pool_id, queue, min_friends, start, end)
    if not data_path.exists():
        print(f"[WARN] Player champions file not found: {data_path}")
        return None

    data = load_data(data_path)
    player_champions = data.get("player_champions")

    if not player_champions:
        return None

    players = sorted(player_champions.keys())

    if player is None:
        return {"players": players}

    pdata = player_champions.get(player)
    if not pdata:
        return {"players": players}

    df = pd.DataFrame(pdata)
    if df.empty:
        return {"players": players}

    df = df.sort_values("winrate", ascending=True)

    fig = make_fig_horizontal(df, f"Winrate de campeones para {player}", player)

    return {"fig": fig, "players": players}
