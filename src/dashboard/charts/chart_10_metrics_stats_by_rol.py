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
    return BASE_DIR / "data" / "results" / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}" / "metrics_10_stats_by_rol.json"


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


# ============================================================
#   FUNCIONES PARA LAS GRÁFICAS
# ============================================================

def make_fig(df: pd.DataFrame, title: str):
    """Figura genérica horizontal."""
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

    fig.update_layout(bargap=0.18)

    fig.update_traces(
        textposition="inside",
        insidetextanchor="middle",
        marker_line_width=0,
        textfont=dict(
            color="white",
            size=max(12, min(20, int(220 / max(1, n))))
        ),
    )

    fig.update_layout(
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


def make_fig_games(df: pd.DataFrame, title: str):
    """Figura para Games con tooltip de % total."""
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
        hover_data={"value": False, "tooltip": True},
    )

    fig.update_layout(bargap=0.18)

    fig.update_traces(
        textposition="inside",
        insidetextanchor="middle",
        marker_line_width=0,
        textfont=dict(
            color="white",
            size=max(12, min(20, int(220 / max(1, n))))
        ),
        customdata=df["tooltip"],
        hovertemplate="<b>%{y}</b><br>Games: %{x}<br>%{customdata}<extra></extra>",
    )

    fig.update_layout(
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


def get_chart_data(raw, selected_role: str):
    """Obtiene datos preparados para un rol específico."""
    if selected_role not in raw:
        return None

    role_data = raw[selected_role]
    
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

    total_games = sum(player["games"] for player in role_data.values())

    for persona, stats in role_data.items():
        games = stats.get("games", 0)
        
        # % del total global
        global_pct = (games / total_games * 100) if total_games > 0 else 0
        
        # Total de games del jugador en TODOS los roles
        player_total_games = 0
        for role in raw.values():
            if persona in role:
                player_total_games += role[persona].get("games", 0)
        
        # % de games en este rol vs sus games totales
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


# ============================================================
#   FUNCIÓN PARA USAR EN TU FLUJO (sin Dash)
# ============================================================

def render(pool_id: str, queue: int, min_friends: int, selected_role: str = None):
    """
    Función que retorna las figuras de Plotly para usar en tu flujo existente.
    Devuelve figuras para el rol seleccionado (si se pasa `selected_role`) o para todos los roles (si no se pasa).

    :param selected_role: Rol a filtrar (por defecto None, lo que generará figuras para todos los roles).
    """
    data_file = get_data_file(pool_id, queue, min_friends)
    raw = load_json(data_file)
    
    if not raw:
        print(f"[ERROR] No se pudieron cargar datos de {data_file}")
        return []
    
    roles = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
    
    # Si se pasa un rol específico, solo procesar ese rol
    if selected_role:
        roles = [selected_role]  # Solo incluir el rol seleccionado
    
    figures = []
    
    for role in roles:
        data = get_chart_data(raw, role)
        
        if data is None:
            continue
        
        # 12 métricas por rol
        figures.append(make_fig(data["winrate"], f"Winrate % - {role}"))
        figures.append(make_fig_games(data["games"], f"Partidas jugadas - {role}"))
        figures.append(make_fig(data["damage"], f"Daño infligido medio - {role}"))
        figures.append(make_fig(data["damage_taken"], f"Daño recibido - {role}"))
        figures.append(make_fig(data["gold"], f"Oro - {role}"))
        figures.append(make_fig(data["farm"], f"CS - {role}"))
        figures.append(make_fig(data["vision"], f"Vision Score - {role}"))
        figures.append(make_fig(data["turret_damage"], f"Daño a torres - {role}"))
        figures.append(make_fig(data["kills"], f"Kills - {role}"))
        figures.append(make_fig(data["deaths"], f"Muertes - {role}"))
        figures.append(make_fig(data["assists"], f"Asistencias - {role}"))
        figures.append(make_fig(data["kill_participation"], f"Kill Participation % - {role}"))
    
    return figures