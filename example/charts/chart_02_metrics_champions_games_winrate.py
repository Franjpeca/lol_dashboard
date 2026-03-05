import json
from pathlib import Path
from typing import Dict, Any, List, Optional
import pandas as pd
import plotly.express as px
import plotly.io as pio
import plotly.graph_objs as go

pio.templates.default = "plotly_dark"

BASE_DIR = Path(__file__).resolve().parents[3]

# ============================================================
#   LOCALIZAR RUTA DEL JSON SEGUN POOL / QUEUE / MIN / FECHAS
# ============================================================

def get_data_file(pool_id: str, queue: int, min_friends: int, start_date=None, end_date=None) -> Path:
    # Si se proporcionan fechas, busca el archivo con el rango de fechas en el nombre
    if start_date and end_date:
        return (
            BASE_DIR
            / "data"
            / "runtime"
            / f"pool_{pool_id}"
            / f"q{queue}"
            / f"min{min_friends}"
            / f"metrics_02_champions_games_winrate_{start_date}_to_{end_date}.json"
        )
    else:
        # Si no se proporcionan fechas, busca el archivo predeterminado
        return (
            BASE_DIR
            / "data"
            / "results"
            / f"pool_{pool_id}"
            / f"q{queue}"
            / f"min{min_friends}"
            / "metrics_02_champions_games_winrate.json"
        )


# ============================================================
#   CARGA
# ============================================================

def load_data(path: Path):
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def df_from_list(raw):
    return pd.DataFrame(raw)


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
        y="champion",
        orientation="h",
        text=x,
        color="games",
        color_continuous_scale="Turbo",
        labels={"games": "Partidas", "winrate": "Winrate (%)"},
        hover_data=["games", "winrate"],
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
            tickvals=df["champion"].tolist(),
            ticktext=df["champion"].tolist(),
            tickfont=dict(size=tick_font_size),
            automargin=True,
        ),
    )

    return fig


# ============================================================
#   RENDER PRINCIPAL PARA DASH
# ============================================================

def render(pool_id: str, queue: int, min_friends: int, start=None, end=None):
    # Usa los parámetros pool_id, queue y min_friends para obtener el archivo de datos
    data_path = get_data_file(pool_id, queue, min_friends, start, end)

    if not data_path.exists():
        print(f"[WARN] champions file not found: {data_path}")
        return []

    data = load_data(data_path)

    # Construcción de DataFrames para los jugadores y enemigos
    df_players = df_from_list(data.get("champions", []))
    df_enemies = df_from_list(data.get("enemy_champions", []))

    figs = []

    # Gráficas para los jugadores
    if not df_players.empty:
        df_players_games = df_players.sort_values("games", ascending=True)
        df_players_wr = df_players.sort_values("winrate", ascending=True)

        figs.append(
            make_fig_horizontal(
                df_players_games,
                "games",
                "Nuestros campeones mas jugados",
            )
        )
        figs.append(
            make_fig_horizontal(
                df_players_wr,
                "winrate",
                "Mejor winrate por campeon (nuestros)",
            )
        )

    # Gráficas para los enemigos
    if not df_enemies.empty:
        df_enemies_games = df_enemies.sort_values("games", ascending=True)
        df_enemies_wr = df_enemies.sort_values("winrate", ascending=True)

        figs.append(
            make_fig_horizontal(
                df_enemies_games,
                "games",
                "Campeones enemigos mas jugados",
            )
        )
        figs.append(
            make_fig_horizontal(
                df_enemies_wr,
                "winrate",
                "Mejor winrate enemigo por campeon",
            )
        )

    return figs


def render_player_champions(pool_id: str, queue: int, min_friends: int, player: Optional[str], start: Optional[str] = None, end: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Renderiza un gráfico de barras con el winrate de campeones para un jugador específico.
    Si el jugador no se especifica, devuelve la lista de jugadores disponibles.
    """
    data_path = get_data_file(pool_id, queue, min_friends, start, end)
    if not data_path.exists():
        return None

    data = load_data(data_path)
    player_champions_data = data.get("player_champions")

    if not player_champions_data:
        return None

    players = sorted(list(player_champions_data.keys()))

    # Si no se especifica un jugador, devolvemos la lista de jugadores para el dropdown
    if player is None:
        return {"players": players}

    player_data = player_champions_data.get(player)
    if not player_data:
        return {"players": players} # Devuelve jugadores aunque el seleccionado no tenga datos

    df = pd.DataFrame(player_data)
    if df.empty:
        return {"players": players}

    df = df.sort_values("games", ascending=True)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df["champion"],
        x=df["winrate"],
        orientation="h",
        text=df.apply(lambda row: f'{row["winrate"]}% ({row["games"]} partidas)', axis=1),
        textposition="outside",
        marker_color=df["games"],
        colorscale="Viridis",
        hovertemplate="<b>%{y}</b><br>Winrate: %{x}%<br>Partidas: %{marker.color}<extra></extra>"
    ))
    fig.update_layout(title=f"Winrate de campeones para {player}", xaxis_title="Winrate (%)", yaxis_title="Campeón", height=max(400, len(df) * 30))
    
    return {"fig": fig, "players": players}
