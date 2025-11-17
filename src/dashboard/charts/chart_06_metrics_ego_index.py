import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.io as pio

pio.templates.default = "plotly_dark"

BASE_DIR = Path(__file__).resolve().parents[3]


# ============================================================
#   LOCALIZAR ARCHIVO SEGÚN pool / queue / min
# ============================================================

def get_data_file(pool_id: str, queue: int, min_friends: int) -> Path:
    """
    Obtiene la ruta del archivo de datos basado en los parámetros proporcionados.
    """
    return BASE_DIR / "data" / "results" / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}" / "metrics_06_ego_index.json"


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


def build_df_ego(data):
    """
    Construye el DataFrame para el ego index.
    """
    rows = []
    for persona, stats in data.items():
        rows.append({
            "persona": persona,
            "ego_index": stats.get("ego_index", 0),
            "selfish_score": stats.get("selfish_score", 0),
            "teamplay_score": stats.get("teamplay_score", 0),
            "tilt_score": stats.get("tilt_score", 0),
            "avg_kills": stats.get("avg_kills", 0),
            "avg_assists": stats.get("avg_assists", 0),
            "avg_deaths": stats.get("avg_deaths", 0),
            "avg_gold": stats.get("avg_gold", 0),
            "avg_damage_dealt": stats.get("avg_damage_dealt", 0),
            "avg_vision_score": stats.get("avg_vision_score", 0),
            "lost_surrender_rate": round(stats.get("lost_surrender_rate", 0) * 100, 2),
            "match_count": stats.get("match_count", 0),
        })
    return pd.DataFrame(rows).sort_values("ego_index")


# ============================================================
#   FUNCIONES PARA LAS GRÁFICAS
# ============================================================

def make_ego_fig(df: pd.DataFrame):
    """
    Genera la figura del Ego Index.
    """
    n = len(df)
    tick_font = max(12, min(22, int(320 / max(1, n))))
    height = int(max(500, min(900, 35 * max(1, n))))

    fig = px.bar(
        df,
        x="ego_index",
        y="persona",
        orientation="h",
        color="ego_index",
        color_continuous_scale="Turbo",
        text="ego_index",
        title="Ego Index por persona",
        hover_data={
            "ego_index": True,
            "selfish_score": True,
            "teamplay_score": True,
            "tilt_score": True,
            "avg_kills": True,
            "avg_assists": True,
            "avg_deaths": True,
            "avg_gold": True,
            "avg_damage_dealt": True,
            "avg_vision_score": True,
            "lost_surrender_rate": True,
            "match_count": True,
        }
    )

    fig.update_traces(
        texttemplate="%{x:.2f}",
        textposition="inside",
        insidetextanchor="middle",
        marker_line_width=0,
    )

    fig.update_layout(
        height=height,
        bargap=0.18,
        margin=dict(l=200, r=60, t=60, b=40),
        xaxis_title="Ego Index",
        yaxis=dict(
            type="category",
            tickfont=dict(size=tick_font),
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
    Sin iniciar servidor Dash.
    """
    data_file = get_data_file(pool_id, queue, min_friends)
    data = load_json(data_file)
    
    if not data:
        print(f"[ERROR] No se pudieron cargar datos de {data_file}")
        return []
    
    df_ego = build_df_ego(data)
    
    if df_ego.empty:
        print("[WARN] DataFrame de ego index está vacío")
        return []
    
    # Retornar lista con una sola figura
    return [
        make_ego_fig(df_ego),
    ]