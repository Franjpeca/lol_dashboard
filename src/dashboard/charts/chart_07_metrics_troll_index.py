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
        return base_path / f"metrics_07_troll_index_{start_date}_to_{end_date}.json"
    return base_path / "metrics_07_troll_index.json"


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


def build_df_early_surrender(data):
    """
    Construye el DataFrame para Early Surrender (métricas relacionadas con surrender temprano).
    """
    rows = []

    for persona, stats in data.items():
        total = stats.get("total_matches", 1)

        # Early surrender propio
        rows.append({
            "persona": persona,
            "metric": "Early FF propio",
            "value": round(stats.get("pct_early_surrender_own", 0) * 100, 3),
            "raw_value": stats.get("early_surrender_own", 0),
            "text_value": f"{round(stats.get('pct_early_surrender_own', 0)*100, 2)}%",
            "total_matches": total,
        })

        # Early surrender enemigo
        rows.append({
            "persona": persona,
            "metric": "Early FF enemigo",
            "value": round(stats.get("pct_early_surrender_enemy", 0) * 100, 3),
            "raw_value": stats.get("early_surrender_enemy", 0),
            "text_value": f"{round(stats.get('pct_early_surrender_enemy', 0)*100, 2)}%",
            "total_matches": total,
        })

    return pd.DataFrame(rows).sort_values("persona")


# ============================================================
#   FUNCIONES PARA LAS GRÁFICAS
# ============================================================

def make_early_surrender_fig(df: pd.DataFrame):
    """
    Genera la figura de "Early Surrender" por persona.
    """
    n = len(df["persona"].unique())
    tick_font = max(12, min(22, int(320 / max(1, n))))
    height = int(max(550, min(1000, 35 * max(1, n))))

    fig = px.bar(
        df,
        x="value",
        y="persona",
        orientation="h",
        color="metric",
        barmode="group",
        text="text_value",
        title="Early Surrender por persona",
        hover_data={
            "value": True,
            "raw_value": True,
            "metric": True,
            "total_matches": True,
        },
    )

    fig.update_traces(
        textposition="inside",
        insidetextanchor="middle",
        marker_line_width=0,
    )

    fig.update_layout(
        height=height,
        bargap=0.20,
        margin=dict(l=180, r=40, t=60, b=40),
        xaxis_title="Porcentaje (%)",
        yaxis=dict(
            type="category",
            tickfont=dict(size=tick_font),
            automargin=True,
        ),
        legend=dict(title="Métrica"),
    )

    return fig


# ============================================================
#   FUNCIÓN PARA USAR EN TU FLUJO (sin Dash)
# ============================================================

def render(pool_id: str, queue: int, min_friends: int, start: str | None = None, end: str | None = None):
    """
    Función que retorna las figuras de Plotly para usar en tu flujo existente.
    Sin iniciar servidor Dash.
    """
    data_file = get_data_file(pool_id, queue, min_friends, start, end)
    raw = load_json(data_file)

    # Extraer los datos del bloque "troll"
    data = raw.get("troll", {})

    if not data:
        print(f"[ERROR] No se pudieron cargar datos reales de {data_file}")
        return []

    df_early = build_df_early_surrender(data)

    if df_early.empty:
        print("[WARN] DataFrame de early surrender está vacío")
        return []

    return [
        make_early_surrender_fig(df_early),
    ]
