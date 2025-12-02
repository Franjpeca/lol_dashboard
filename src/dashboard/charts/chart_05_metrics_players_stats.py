import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go

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
        return base_path / f"metrics_05_players_stats_{start_date}_to_{end_date}.json"
    return base_path / "metrics_05_players_stats.json"


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


def build_dataframes(raw):
    players = raw.get("players", {})

    rows_kda = []
    rows_kills = []
    rows_deaths = []
    rows_assists = []
    rows_gold = []
    rows_damage_dealt = []
    rows_damage_taken = []
    rows_vision = []

    for persona, stats in players.items():
        if not isinstance(stats, dict):
            continue
        
        rows_kda.append({"persona": persona, "value": stats.get("avg_kda", 0)})
        rows_kills.append({"persona": persona, "value": stats.get("avg_kills", 0)})
        rows_deaths.append({"persona": persona, "value": stats.get("avg_deaths", 0)}) # 'value' is correct here for sorting
        rows_assists.append({"persona": persona, "value": stats.get("avg_assists", 0)})
        rows_gold.append({"persona": persona, "value": stats.get("avg_gold", 0)})
        rows_damage_dealt.append({"persona": persona, "value": stats.get("avg_damage_dealt", 0)})
        rows_damage_taken.append({"persona": persona, "value": stats.get("avg_damage_taken", 0)})
        rows_vision.append({"persona": persona, "value": stats.get("avg_vision_score", 0)})

    return {
        "kda": pd.DataFrame(rows_kda).sort_values("value"),
        "kills": pd.DataFrame(rows_kills).sort_values("value"),
        "deaths": pd.DataFrame(rows_deaths).sort_values("value"),
        "assists": pd.DataFrame(rows_assists).sort_values("value"),
        "gold": pd.DataFrame(rows_gold).sort_values("value"),
        "damage_dealt": pd.DataFrame(rows_damage_dealt).sort_values("value"),
        "damage_taken": pd.DataFrame(rows_damage_taken).sort_values("value"),
        "vision": pd.DataFrame(rows_vision).sort_values("value"),
    }


# ============================================================
#   FUNCIONES PARA LAS GRÁFICAS
# ============================================================

def make_fig(df: pd.DataFrame, title: str):
    """
    Genera una figura genérica con el estilo del script original.
    """
    n = len(df)
    tick_font = max(12, min(22, int(320 / max(1, n))))
    height = int(max(550, min(1000, 35 * max(1, n))) * 0.75)

    fig = px.bar(
        df,
        x="value",
        y="persona",
        orientation="h",
        text="value",
        color="value",
        color_continuous_scale="Turbo",
        title=title,
    )

    fig.update_traces(
        textposition="inside",
        insidetextanchor="middle",
        marker_line_width=0,
        textfont=dict(color="white", size=max(12, min(20, int(220 / max(1, n))))),
    )

    fig.update_layout(
        bargap=0.18,
        autosize=True,
        height=height + 90,
        margin=dict(l=170, r=50, t=60, b=40),
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


def make_kda_fig(df):
    """Genera la figura de KDA medio."""
    return make_fig(df["kda"], "KDA medio global por persona")


def make_kills_fig(df):
    """Genera la figura de kills promedio."""
    return make_fig(df["kills"], "Kills promedio por partida")


def make_deaths_fig(df):
    """Genera la figura de muertes promedio."""
    return make_fig(df["deaths"], "Muertes promedio por partida")


def make_assists_fig(df):
    """Genera la figura de asistencias promedio."""
    return make_fig(df["assists"], "Asistencias promedio por partida")


def make_gold_fig(df):
    """Genera la figura de oro medio."""
    return make_fig(df["gold"], "Oro medio por partida")


def make_damage_dealt_fig(df):
    """Genera la figura de daño infligido."""
    return make_fig(df["damage_dealt"], "Daño infligido medio")


def make_damage_taken_fig(df):
    """Genera la figura de daño recibido."""
    return make_fig(df["damage_taken"], "Daño recibido medio")


def make_vision_fig(df):
    """Genera la figura de vision score."""
    return make_fig(df["vision"], "Vision score medio")


def make_polyhedron(df: pd.DataFrame, title: str):
    """
    Crea un gráfico de radar 2D (pentagrama/polígono) para comparar jugadores.
    Copiado y adaptado de chart_10.
    """
    fig = go.Figure()

    # Definir las categorías (los ejes del radar) y las columnas correspondientes en el DataFrame
    categories = {
        "KDA": "kda",
        "Asesinatos": "kills",
        "Muertes": "deaths",
        "Asistencias": "assists",
        "Oro": "gold",
        "Daño Infligido": "damage_dealt",
        "Daño Recibido": "damage_taken",
        "Puntuación de Visión": "vision",
    }

    # Normalizar los datos para que estén en una escala comparable
    df_normalized = df.copy()
    for col in categories.values():
        min_val, max_val = df_normalized[col].min(), df_normalized[col].max()
        if (max_val - min_val) > 0:
            # Escalar de 0.2 a 1 para evitar que el valor mínimo sea 0 en el gráfico.
            # Para 'deaths' y 'damage_taken', invertimos la escala (menos es mejor).
            if col in ["deaths", "damage_taken"]:
                 df_normalized[col] = 0.2 + 0.8 * (max_val - df_normalized[col]) / (max_val - min_val)
            else:
                 df_normalized[col] = 0.2 + 0.8 * (df_normalized[col] - min_val) / (max_val - min_val)
        else:
            df_normalized[col] = 0.5  # Si todos los valores son iguales

    # Iterar sobre cada jugador para añadir su "pentagrama" al gráfico
    for _, row in df_normalized.iterrows():
        values = [row[col] for col in categories.values()]
        fig.add_trace(go.Scatterpolar(
            r=values,
            theta=list(categories.keys()),
            fill='toself',
            name=row["persona"],
            hovertemplate=(
                f"<b>{row['persona']}</b><br>" +
                "<br>".join([f"{cat}: {df.loc[_, col_name]:.2f}" for cat, col_name in categories.items()]) +
                "<extra></extra>"
            )
        ))

    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1.05])),
        showlegend=True,
        title=title,
        height=700
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
    
    dfs = build_dataframes(data)
    
    # 1. Crear las figuras de barras
    figures = [
        make_kda_fig(dfs),
        make_kills_fig(dfs),
        make_deaths_fig(dfs),
        make_assists_fig(dfs),
        make_gold_fig(dfs),
        make_damage_dealt_fig(dfs),
        make_damage_taken_fig(dfs),
        make_vision_fig(dfs),
    ]

    # 2. Preparar el DataFrame para el gráfico de radar
    # Renombrar la columna 'value' en cada DataFrame antes de unir
    dfs_to_merge = [df.rename(columns={"value": name}) for name, df in dfs.items()]
    poly_df = dfs_to_merge[0]
    for df_to_merge in dfs_to_merge[1:]:
        poly_df = pd.merge(poly_df, df_to_merge, on="persona", how="outer")
    poly_df.fillna(0, inplace=True)

    # 3. Crear y añadir el gráfico de radar
    figures.append(make_polyhedron(poly_df, "Poliedro de Estadísticas Globales"))

    return figures