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
        return base_path / f"metrics_10_stats_by_rol_{start_date}_to_{end_date}.json"
    return base_path / "metrics_10_stats_by_rol.json"


def load_json(path: Path):
    """
    Carga los datos JSON desde la ruta proporcionada.
    """
    if not path.exists():
        print(f"[ERROR] Archivo no encontrado: {path}")
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# Función para crear el gráfico 3D (poliedro)
def make_polyhedron(df: pd.DataFrame, title: str):
    """
    Crea un gráfico de radar 2D (pentagrama/polígono) para comparar jugadores.
    """
    fig = go.Figure()

    # Definir las categorías (los ejes del radar) y las columnas correspondientes en el DataFrame
    categories = {
        "Daño Medio": "avg_damage",
        "Daño Recibido": "avg_damage_taken",
        "Oro Medio": "avg_gold",
        "Farm Medio": "avg_farm",
        "Visión Media": "avg_vision",
        "Daño a torretas": "avg_turret_damage",
        "Asesinatos": "avg_kills",
        "Muertes": "avg_deaths",
    }

    # Normalizar los datos para que estén en una escala comparable (0 a 1)
    # Esto es crucial para que una métrica con valores grandes (como el daño) no domine el gráfico.
    df_normalized = df.copy()
    for col in categories.values():
        min_val, max_val = df_normalized[col].min(), df_normalized[col].max()
        if (max_val - min_val) > 0:
            # Escalar de 0.2 a 1 en lugar de 0 a 1 para evitar que el valor mínimo sea 0 en el gráfico.
            # Esto hace que la forma del polígono sea más representativa.
            df_normalized[col] = 0.2 + 0.8 * (df_normalized[col] - min_val) / (max_val - min_val)
        else:
            df_normalized[col] = 0.5  # Si todos los valores son iguales, se asigna un valor intermedio

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
        polar=dict(radialaxis=dict(visible=True, range=[0, 1.05])), # Aumentar el rango para que el '1' no esté en el borde
        showlegend=True,
        title=title,
        height=700
    )
    return fig


def make_fig(df: pd.DataFrame, title: str):
    """
    Genera una figura genérica con el estilo original.
    """
    n = len(df)
    tick_font = max(12, min(22, int(320 / max(1, n))))
    height = int(max(550, min(1000, 35 * max(1, n))) * 0.75)
    fig = px.bar(df, x="value", y="persona", orientation="h", text="value", color="value", color_continuous_scale="Turbo", title=title)
    fig.update_layout(bargap=0.18)
    fig.update_traces(textposition="inside", insidetextanchor="middle", marker_line_width=0, textfont=dict(color="white", size=max(12, min(20, int(220 / max(1, n))))) )
    fig.update_layout(autosize=True, height=height + 90, margin=dict(l=170, r=50, t=60, b=40), xaxis_title="", yaxis=dict(type="category", tickvals=df["persona"].tolist(), ticktext=df["persona"].tolist(), tickfont=dict(size=tick_font), automargin=True))
    return fig


def make_fig_games(df: pd.DataFrame, title: str):
    """
    Genera una figura de barras para mostrar las partidas jugadas.
    """
    n = len(df)
    tick_font = max(12, min(22, int(320 / max(1, n))))
    height = int(max(550, min(1000, 35 * max(1, n))) * 0.75)
    fig = px.bar(df, x="value", y="persona", orientation="h", text="value", color="value", color_continuous_scale="Turbo", title=title, hover_data={"value": False, "tooltip": True})
    fig.update_layout(bargap=0.18)
    fig.update_traces(textposition="inside", insidetextanchor="middle", marker_line_width=0, textfont=dict(color="white", size=max(12, min(20, int(220 / max(1, n))))) , customdata=df["tooltip"], hovertemplate="<b>%{y}</b><br>Games: %{x}<br>%{customdata}<extra></extra>")
    fig.update_layout(autosize=True, height=height + 90, margin=dict(l=170, r=50, t=60, b=40), xaxis_title="", yaxis=dict(type="category", tickvals=df["persona"].tolist(), ticktext=df["persona"].tolist(), tickfont=dict(size=tick_font), automargin=True))
    return fig


def get_chart_data(raw, selected_role: str, min_games: int = 0):
    """
    Extrae y filtra los datos de acuerdo con el rol seleccionado y la cantidad mínima de juegos.
    """
    if selected_role not in raw:
        return None

    role_data = raw[selected_role]

    # Filtrado de jugadores con el mínimo de juegos
    filtered = {
        persona: stats
        for persona, stats in role_data.items()
        if stats.get("games", 0) >= min_games
    }

    # Si no hay jugadores que cumplan el filtro, no se genera ninguna gráfica
    if not filtered:
        return None

    rows_winrate = []
    rows_games = []
    rows_damage = []
    rows_damage_taken = []
    rows_gold = []
    rows_farm = []
    rows_vision = []
    rows_turret_damage = []
    rows_kills = []
    rows_deaths = []
    rows_assists = []
    rows_kill_participation = []

    total_games = sum(stats["games"] for stats in filtered.values())

    for persona, stats in filtered.items():
        games = stats["games"]

        global_pct = (games / total_games * 100) if total_games > 0 else 0

        # Total de juegos en todos los roles para calcular el porcentaje del rol del jugador
        player_total_games = sum(
            r.get(persona, {}).get("games", 0)
            for r in raw.values()
        )

        player_role_pct = (games / player_total_games * 100) if player_total_games > 0 else 0

        rows_winrate.append({"persona": persona, "value": stats.get("winrate", 0)})
        rows_games.append({
            "persona": persona,
            "value": games,
            "tooltip": f"{global_pct:.1f}% en global | {player_role_pct:.1f}% en sus partidas"
        })
        rows_damage.append({"persona": persona, "value": stats.get("avg_damage", 0)})
        rows_damage_taken.append({"persona": persona, "value": stats.get("avg_damage_taken", 0)})
        rows_gold.append({"persona": persona, "value": stats.get("avg_gold", 0)})
        rows_farm.append({"persona": persona, "value": stats.get("avg_farm", 0)})
        rows_vision.append({"persona": persona, "value": stats.get("avg_vision", 0)})
        rows_turret_damage.append({"persona": persona, "value": stats.get("avg_turret_damage", 0)})
        rows_kills.append({"persona": persona, "value": stats.get("avg_kills", 0)})
        rows_deaths.append({"persona": persona, "value": stats.get("avg_deaths", 0)})
        rows_assists.append({"persona": persona, "value": stats.get("avg_assists", 0)})
        rows_kill_participation.append({"persona": persona, "value": stats.get("avg_kill_participation", 0)})

    return {
        "winrate": pd.DataFrame(rows_winrate).sort_values("value"),
        "games": pd.DataFrame(rows_games).sort_values("value"),
        "damage": pd.DataFrame(rows_damage).sort_values("value"),
        "damage_taken": pd.DataFrame(rows_damage_taken).sort_values("value"),
        "gold": pd.DataFrame(rows_gold).sort_values("value"),
        "farm": pd.DataFrame(rows_farm).sort_values("value"),
        "vision": pd.DataFrame(rows_vision).sort_values("value"),
        "turret_damage": pd.DataFrame(rows_turret_damage).sort_values("value"),
        "kills": pd.DataFrame(rows_kills).sort_values("value"),
        "deaths": pd.DataFrame(rows_deaths).sort_values("value"),
        "assists": pd.DataFrame(rows_assists).sort_values("value"),
        "kill_participation": pd.DataFrame(rows_kill_participation).sort_values("value"),
    }


def render(pool_id: str, queue: int, min_friends: int, selected_role: str = None, min_games: int = 0, start: str | None = None, end: str | None = None):
    """
    Función que retorna las figuras de Plotly para usar en tu flujo existente.
    """
    data_file = get_data_file(pool_id, queue, min_friends, start, end)
    raw = load_json(data_file)
    if not raw:
        print(f"[ERROR] No se pudieron cargar datos de {data_file}")
        return []

    # Extraer el diccionario de roles del JSON
    roles_data = raw.get("roles", {})

    roles = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
    if selected_role:
        roles = [selected_role]

    figures = []
    for role in roles:
        data = get_chart_data(roles_data, role, min_games)
        if data is None:
            continue

        # 1. Añadir las gráficas de barras para winrate y partidas
        figures.append(make_fig(data["winrate"], f"Winrate - {role}"))
        figures.append(make_fig_games(data["games"], f"Partidas Jugadas - {role}"))

        # 2. Preparar el DataFrame para el gráfico de radar (todas las demás métricas)
        radar_metrics = {
            "avg_damage": data["damage"],
            "avg_damage_taken": data["damage_taken"],
            "avg_gold": data["gold"],
            "avg_farm": data["farm"],
            "avg_vision": data["vision"],
            "avg_turret_damage": data["turret_damage"],
            "avg_kills": data["kills"],
            "avg_deaths": data["deaths"],
        }

        # Renombrar y unir los DataFrames para el radar
        dfs_to_merge = [df.rename(columns={"value": name}) for name, df in radar_metrics.items()]
        poly_df = dfs_to_merge[0]
        for df_to_merge in dfs_to_merge[1:]:
            poly_df = pd.merge(poly_df, df_to_merge, on="persona", how="outer")

        # Rellenar valores nulos con 0
        poly_df.fillna(0, inplace=True)

        # 3. Crear y añadir el gráfico de radar
        polyhedron_fig = make_polyhedron(poly_df, f"Poliedro de Estadísticas - {role}")
        figures.append(polyhedron_fig)

    return figures
