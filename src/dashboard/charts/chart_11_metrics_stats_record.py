import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.io as pio
import re

pio.templates.default = "plotly_dark"

BASE_DIR = Path(__file__).resolve().parents[3]


def get_data_file(pool_id: str, queue: int, min_friends: int) -> Path:
    return BASE_DIR / "data" / "results" / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}" / "metrics_11_stats_record.json"


def load_json(path: Path):
    if not path.exists():
        print(f"[ERROR] Archivo no encontrado: {path}")
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def convert_to_seconds(longest_game_str):
    numbers = re.findall(r'(\d+)', longest_game_str)
    if len(numbers) == 2:
        m, s = map(int, numbers)
        return m * 60 + s
    if len(numbers) == 1:
        return int(numbers[0]) * 60
    return 0


def build_df_records(data):
    rows = []
    for persona, stats in data.items():
        rows.append({
            "persona": persona,
            "max_kills": stats.get("max_kills", {}).get("value", 0),
            "max_kills_id": stats.get("max_kills", {}).get("game_id"),

            "max_deaths": stats.get("max_deaths", {}).get("value", 0),
            "max_deaths_id": stats.get("max_deaths", {}).get("game_id"),

            "max_assists": stats.get("max_assists", {}).get("value", 0),
            "max_assists_id": stats.get("max_assists", {}).get("game_id"),

            "max_vision_score": stats.get("max_vision_score", {}).get("value", 0),
            "max_vision_score_id": stats.get("max_vision_score", {}).get("game_id"),

            "max_farm": stats.get("max_farm", {}).get("value", 0),
            "max_farm_id": stats.get("max_farm", {}).get("game_id"),

            "max_damage_dealt": stats.get("max_damage_dealt", {}).get("value", 0),
            "max_damage_dealt_id": stats.get("max_damage_dealt", {}).get("game_id"),

            "max_gold": stats.get("max_gold", {}).get("value", 0),
            "max_gold_id": stats.get("max_gold", {}).get("game_id"),

            "longest_game": convert_to_seconds(stats.get("longest_game", {}).get("value", "0m 0s")),
            "longest_game_id": stats.get("longest_game", {}).get("game_id"),
        })
    return pd.DataFrame(rows)


def make_fig_horizontal(df: pd.DataFrame, x: str, title: str, match_ids):
    n = len(df)
    tick_font_size = max(12, min(22, int(320 / n)))
    base_height = max(750, min(1200, 35 * n))
    fig_height = int(base_height * 0.75)

    fig = px.bar(
        df,
        x=x,
        y="persona",
        orientation="h",
        text=x,
        color=x,
        color_continuous_scale="Turbo",
        title=title,
    )

    fig.update_traces(
        customdata=match_ids,
        hovertemplate="<b>%{y}</b><br>%{x}<br>ID: %{customdata}<extra></extra>",
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(color="white", size=max(12, min(20, int(220 / n)))),
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
            tickvals=df["persona"].tolist(),
            ticktext=df["persona"].tolist(),
            tickfont=dict(size=tick_font_size),
            automargin=True,
        ),
    )

    return fig


def render(pool_id: str, queue: int, min_friends: int):
    data_file = get_data_file(pool_id, queue, min_friends)
    data = load_json(data_file)

    if not data:
        print(f"[ERROR] No se pudieron cargar datos de {data_file}")
        return []

    df = build_df_records(data)

    if df.empty:
        print("[WARN] DataFrame vacio")
        return []

    return [
        {
            "fig": make_fig_horizontal(
                df.sort_values("max_kills"),
                "max_kills",
                "Maximos kills",
                df.sort_values("max_kills")["max_kills_id"].tolist()
            )
        },
        {
            "fig": make_fig_horizontal(
                df.sort_values("max_deaths"),
                "max_deaths",
                "Maximas muertes",
                df.sort_values("max_deaths")["max_deaths_id"].tolist()
            )
        },
        {
            "fig": make_fig_horizontal(
                df.sort_values("max_assists"),
                "max_assists",
                "Maximas asistencias",
                df.sort_values("max_assists")["max_assists_id"].tolist()
            )
        },
        {
            "fig": make_fig_horizontal(
                df.sort_values("max_vision_score"),
                "max_vision_score",
                "Maxima vision",
                df.sort_values("max_vision_score")["max_vision_score_id"].tolist()
            )
        },
        {
            "fig": make_fig_horizontal(
                df.sort_values("max_farm"),
                "max_farm",
                "Maximo farm",
                df.sort_values("max_farm")["max_farm_id"].tolist()
            )
        },
        {
            "fig": make_fig_horizontal(
                df.sort_values("max_damage_dealt"),
                "max_damage_dealt",
                "Maximo dano infligido",
                df.sort_values("max_damage_dealt")["max_damage_dealt_id"].tolist()
            )
        },
        {
            "fig": make_fig_horizontal(
                df.sort_values("max_gold"),
                "max_gold",
                "Maximo oro",
                df.sort_values("max_gold")["max_gold_id"].tolist()
            )
        },
        {
            "fig": make_fig_horizontal(
                df.sort_values("longest_game"),
                "longest_game",
                "Partida mas larga",
                df.sort_values("longest_game")["longest_game_id"].tolist()
            )
        },
    ]
