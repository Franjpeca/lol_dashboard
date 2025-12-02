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

def get_data_file(pool_id: str, queue: int, min_friends: int, start_date: str | None = None, end_date: str | None = None) -> Path:
    """
    Obtiene la ruta del archivo de datos basado en los parámetros proporcionados.
    """
    base_path = BASE_DIR / "data" / ("runtime" if start_date and end_date else "results") / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}"
    if start_date and end_date:
        return base_path / f"metrics_08_first_metrics_{start_date}_to_{end_date}.json"
    return base_path / "metrics_08_first_metrics.json"


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

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        # Si viene en formato envoltorio (source_L1, generated_at, etc.)
        # nos quedamos SOLO con la clave de métricas.
        for key in ["first_metrics", "ego_index", "troll_index", "players_stats", "win_lose_streak"]:
            if key in data and isinstance(data[key], dict):
                return data[key]

        return data

    except Exception as e:
        print(f"[ERROR] No se pudo leer JSON: {e}")
        return {}


def build_df_first_metrics(data):
    """
    Construye el DataFrame completo con todas las métricas.
    """
    rows = []
    
    for persona, stats in data.items():
        rows.append({
            "persona": persona,
            "match_count": stats.get("match_count", 0),
            
            "fb_kills": stats.get("first_blood_kills", 0),
            "fb_kills_pct": stats.get("first_blood_kills_rate", 0.0) * 100,
            
            "fb_assists": stats.get("first_blood_assists", 0),
            "fb_assists_pct": stats.get("first_blood_assists_rate", 0.0) * 100,
            
            "fd_count": stats.get("first_death_count", 0),
            "fd_pct": stats.get("first_death_rate", 0.0) * 100,
        })
    
    return pd.DataFrame(rows)


# ============================================================
#   FUNCIONES PARA LAS GRÁFICAS
# ============================================================

def make_fig(df: pd.DataFrame, title: str, percent: bool = False, hover_cols=None):
    """
    Genera una figura genérica.
    """
    n = len(df)
    tick_font = max(12, min(22, int(320 / max(1, n))))
    height = int(max(550, min(1000, 35 * max(1, n))) * 0.85)

    if hover_cols is None:
        hover_cols = ["value", "match_count"]

    fig = px.bar(
        df,
        x="value",
        y="persona",
        orientation="h",
        text="value",
        color="value",
        color_continuous_scale="Turbo",
        hover_data=hover_cols,
        title=title,
    )

    fig.update_layout(bargap=0.18)

    texttemplate = "%{text:.1f}%" if percent else "%{text:.0f}"

    fig.update_traces(
        textposition="inside",
        insidetextanchor="middle",
        texttemplate=texttemplate,
        marker_line_width=0,
        textfont=dict(color="white", size=max(12, min(20, int(220 / max(1, n))))),
    )

    fig.update_layout(
        autosize=True,
        height=height + 90,
        margin=dict(l=200, r=40, t=60, b=40),
        xaxis_title="",
        yaxis=dict(
            type="category",
            tickvals=df["persona"].tolist(),
            ticktext=df["persona"].tolist(),
            tickfont=dict(size=tick_font),
            automargin=True,
        ),
    )

    return fig


def prep(df, abs_col, pct_col):
    """
    Prepara DataFrames para valor absoluto y porcentaje.
    """
    df_abs = df[["persona", "match_count", abs_col]].rename(columns={abs_col: "value"})
    df_abs = df_abs.sort_values("value")

    df_pct = df[["persona", "match_count", pct_col]].rename(columns={pct_col: "value"})
    df_pct = df_pct.sort_values("value")

    return df_abs, df_pct


# ============================================================
#   FUNCIÓN PARA USAR EN TU FLUJO (sin Dash)
# ============================================================

def render(pool_id: str, queue: int, min_friends: int, start: str | None = None, end: str | None = None):
    """
    Función que retorna las figuras de Plotly para usar en tu flujo existente.
    Sin iniciar servidor Dash.
    Devuelve 6 figuras (porcentaje + absoluto).
    """
    data_file = get_data_file(pool_id, queue, min_friends, start, end)
    data = load_json(data_file)

    if not data:
        print(f"[ERROR] No se pudieron cargar datos de {data_file}")
        return []

    # 🔥 FIX IMPORTANTE: filtrar el JSON para quedarnos con "first_metrics"
    if "first_metrics" in data:
        data = data["first_metrics"]

    df_all = build_df_first_metrics(data)

    if df_all.empty:
        print("[WARN] DataFrame de first metrics está vacío")
        return []

    # Preparar DataFrames
    df_fb_k_abs, df_fb_k_pct = prep(df_all, "fb_kills", "fb_kills_pct")
    df_fb_a_abs, df_fb_a_pct = prep(df_all, "fb_assists", "fb_assists_pct")
    df_fd_abs, df_fd_pct = prep(df_all, "fd_count", "fd_pct")

    # Retornar lista de figuras
    return [
        make_fig(df_fb_k_abs, "Veces con First Blood (Kill)"),
        make_fig(df_fb_k_pct, "Porcentaje con First Blood (Kill)", percent=True),

        make_fig(df_fb_a_abs, "Asistencias en First Blood"),
        make_fig(df_fb_a_pct, "Porcentaje de Asistencias en First Blood", percent=True),

        make_fig(df_fd_abs, "Veces que muere primero"),
        make_fig(df_fd_pct, "Porcentaje de primeras muertes", percent=True),
    ]
