import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.io as pio
import re

pio.templates.default = "plotly_dark"

BASE_DIR = Path(__file__).resolve().parents[3]


# ============================================================
#   LOCALIZAR ARCHIVO SEGÚN pool / queue / min
# ============================================================

def get_data_file(pool_id: str, queue: int, min_friends: int) -> Path:
    """
    Obtiene la ruta del archivo de datos basado en los parámetros proporcionados.
    """
    return BASE_DIR / "data" / "results" / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}" / "metrics_11_stats_record.json"


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


def convert_to_seconds(longest_game_str):
    """Convierte el tiempo en formato 'Xm Ys' a segundos."""
    time_parts = re.findall(r'(\d+)', longest_game_str)
    if len(time_parts) == 2:
        minutes, seconds = map(int, time_parts)
        return minutes * 60 + seconds
    elif len(time_parts) == 1:
        return int(time_parts[0]) * 60  # Si solo hay minutos, convertir a segundos
    return 0  # Si el formato no es válido, devolver 0


def build_df_records(data):
    """
    Construye el DataFrame con todos los récords.
    """
    rows = []
    for persona, stats in data.items():
        rows.append({
            "persona": persona,
            "max_kills": stats.get("max_kills", {}).get("value", 0),
            "max_deaths": stats.get("max_deaths", {}).get("value", 0),
            "max_assists": stats.get("max_assists", {}).get("value", 0),
            "max_vision_score": stats.get("max_vision_score", {}).get("value", 0),
            "max_farm": stats.get("max_farm", {}).get("value", 0),
            "max_damage_dealt": stats.get("max_damage_dealt", {}).get("value", 0),
            "max_gold": stats.get("max_gold", {}).get("value", 0),
            "longest_game": convert_to_seconds(stats.get("longest_game", {}).get("value", "0m 0s")),
        })
    return pd.DataFrame(rows)


# ============================================================
#   FUNCIONES PARA LAS GRÁFICAS
# ============================================================

def make_fig_horizontal(df: pd.DataFrame, x: str, title: str):
    """
    Genera un gráfico de barras horizontal basado en el DataFrame de jugadores.
    """
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
        hover_data=["max_kills", "max_deaths", "max_assists", "max_vision_score"],
        title=title,
    )

    fig.update_layout(bargap=0.18)

    fig.update_traces(
        textposition="inside",
        insidetextanchor="middle",
        textfont=dict(
            color="white",
            size=max(12, min(20, int(220 / n)))
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
            tickvals=df["persona"].tolist(),
            ticktext=df["persona"].tolist(),
            tickfont=dict(size=tick_font_size),
            automargin=True,
        ),
    )

    return fig


# ============================================================
#   FUNCIÓN PARA USAR EN TU FLUJO (sin Dash)
# ============================================================

def render(pool_id: str, queue: int, min_friends: int):
    """
    Función que retorna las figuras de Plotly para usar en tu flujo existente.
    Devuelve 8 figuras (una por cada métrica de récord).
    """
    data_file = get_data_file(pool_id, queue, min_friends)
    data = load_json(data_file)
    
    if not data:
        print(f"[ERROR] No se pudieron cargar datos de {data_file}")
        return []
    
    df_players = build_df_records(data)
    
    if df_players.empty:
        print("[WARN] DataFrame de récords está vacío")
        return []
    
    # Retornar lista con 8 figuras (una por cada métrica)
    return [
        make_fig_horizontal(
            df_players.sort_values("max_kills", ascending=True),
            "max_kills",
            "Máximos kills de los jugadores"
        ),
        make_fig_horizontal(
            df_players.sort_values("max_deaths", ascending=True),
            "max_deaths",
            "Máximas muertes de los jugadores"
        ),
        make_fig_horizontal(
            df_players.sort_values("max_assists", ascending=True),
            "max_assists",
            "Máximas asistencias de los jugadores"
        ),
        make_fig_horizontal(
            df_players.sort_values("max_vision_score", ascending=True),
            "max_vision_score",
            "Máximo puntaje visión de los jugadores"
        ),
        make_fig_horizontal(
            df_players.sort_values("max_farm", ascending=True),
            "max_farm",
            "Máximo farm de los jugadores"
        ),
        make_fig_horizontal(
            df_players.sort_values("max_damage_dealt", ascending=True),
            "max_damage_dealt",
            "Máximo daño de los jugadores"
        ),
        make_fig_horizontal(
            df_players.sort_values("max_gold", ascending=True),
            "max_gold",
            "Máximo oro de los jugadores"
        ),
        make_fig_horizontal(
            df_players.sort_values("longest_game", ascending=True),
            "longest_game",
            "Partida más larga de los jugadores"
        ),
    ]