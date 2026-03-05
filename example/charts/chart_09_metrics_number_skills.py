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
        return base_path / f"metrics_09_number_skills_{start_date}_to_{end_date}.json"
    return base_path / "metrics_09_number_skills.json"


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


def build_df_skills(data):
    """
    Construye el DataFrame para las habilidades.
    """
    rows = []
    for player, vals in data.items():
        rows.append({"player": player, "skill": "Q", "value": vals.get("avg_Q", 0)})
        rows.append({"player": player, "skill": "W", "value": vals.get("avg_W", 0)})
        rows.append({"player": player, "skill": "E", "value": vals.get("avg_E", 0)})
        rows.append({"player": player, "skill": "R", "value": vals.get("avg_R", 0)})

    df = pd.DataFrame(rows)

    df["player_order"] = df["player"].astype("category").cat.codes
    df["skill_order"] = df["skill"].map({"Q": 0, "W": 1, "E": 2, "R": 3})

    df["x"] = df["player_order"] * 5 + df["skill_order"]
    return df


# ============================================================
#   FUNCIONES PARA LAS GRÁFICAS
# ============================================================

def make_skills_fig(df: pd.DataFrame):
    """
    Genera la figura de uso de habilidades.
    """
    tick_vals = []
    tick_text = []
    offset = 1.5

    for player, grp in df.groupby("player"):
        x_base = grp["x"].min()
        tick_vals.append(x_base + offset)
        tick_text.append(player)

    fig = px.bar(
        df,
        x="x",
        y="value",
        color="skill",
        barmode="group",
        title="Media de usos de habilidades por jugador"
    )

    fig.update_layout(
        xaxis=dict(
            tickmode="array",
            tickvals=tick_vals,
            ticktext=tick_text
        ),
        yaxis_title="Media de usos",
        xaxis_title="Jugador",
        
        bargap=0.45,        # separación entre grupos
        bargroupgap=0.01,   # Q-W-E-R súper pegadas
        
        height=600
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
    data = load_json(data_file)
    
    if not data:
        print(f"[ERROR] No se pudieron cargar datos de {data_file}")
        return []

    # 🔥 FIX IMPORTANTE: extraer el diccionario "skills"
    if "skills" in data:
        data = data["skills"]

    df_skills = build_df_skills(data)
    
    if df_skills.empty:
        print("[WARN] DataFrame de skills está vacío")
        return []
    
    return [
        make_skills_fig(df_skills),
    ]
