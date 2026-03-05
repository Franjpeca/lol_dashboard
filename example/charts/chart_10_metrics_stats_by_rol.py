import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.io as pio
import plotly.graph_objects as go

pio.templates.default = "plotly_dark"

BASE_DIR = Path(__file__).resolve().parents[3]

# ============================================================
# LOCALIZAR ARCHIVO SEGUN pool / queue / min
# ============================================================
def get_data_file(pool_id: str, queue: int, min_friends: int,
                  start_date: str | None = None, end_date: str | None = None) -> Path:
    base_path = BASE_DIR / "data" / ("runtime" if start_date and end_date else "results") \
                / f"pool_{pool_id}" / f"q{queue}" / f"min{min_friends}"
    if start_date and end_date:
        return base_path / f"metrics_10_stats_by_rol_{start_date}_to_{end_date}.json"
    return base_path / "metrics_10_stats_by_rol.json"


def load_json(path: Path):
    if not path.exists():
        print(f"[ERROR] Archivo no encontrado: {path}")
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# GRAFICO RADAR (POLIEDRO) DATOS POR ROL
# ============================================================
def make_polyhedron(df: pd.DataFrame, title: str):
    """
    Crea un gráfico de radar usando Z-scores (normalización estadística).
    - 0.5 = Media del grupo
    - > 0.5 = Por encima de la media
    - < 0.5 = Por debajo de la media
    """
    fig = go.Figure()

    categories = {
        "Daño Medio": "avg_damage",
        "Daño Recibido": "avg_damage_taken",
        "Oro Medio": "avg_gold",
        "Farm Medio": "avg_farm",
        "Visión Media": "avg_vision",
        "Daño a Torretas": "avg_turret_damage",
        "Asesinatos": "avg_kills",
        "Muertes": "avg_deaths",
    }
    
    df_normalized = df.copy()
    references = {}
    
    # Calcular Z-scores para cada métrica
    for col in categories.values():
        if col not in df.columns:
            continue
            
        raw_values = df[col]
        mean = raw_values.mean()
        std = raw_values.std()
        
        # Evitar división por cero
        if std == 0:
            std = 1
        
        references[col] = {"mean": mean, "std": std}
        
        # Z-score DIRECTO para todas las métricas (como en chart_05)
        # Más valor = Más alejado del centro
        z_scores = (raw_values - mean) / std
        
        # Transformar a radar value [0, 1] con 0.5 = media
        df_normalized[col + "_norm"] = (0.5 + z_scores / 4).clip(0, 1)
        df_normalized[col + "_z"] = z_scores

    # Orden de categorías
    cat_keys = list(categories.keys())
    
    # 1. Línea de referencia (Media = 0.5)
    fig.add_trace(go.Scatterpolar(
        r=[0.5] * len(cat_keys),
        theta=cat_keys,
        mode='lines',
        name='Media del Grupo',
        line=dict(color='white', width=2, dash='dash'),
        hoverinfo='skip',
        showlegend=True
    ))
    
    # 2. Dibujar jugadores
    for _, row in df_normalized.iterrows():
        r_values = [row[col + "_norm"] for col in categories.values()]
        
        # Tooltip mejorado con valores reales y Z-scores
        hovertemplate = f"<b>{row['persona']}</b><br><br>"
        for cat, col in categories.items():
            val = df.loc[_, col]
            z = row.get(col + "_z", 0)
            
            # Formateo según métrica
            if col in ["avg_damage", "avg_damage_taken", "avg_gold", "avg_turret_damage"]:
                val_fmt = f"{val:,.0f}"
            else:
                val_fmt = f"{val:.2f}"
            
            hovertemplate += f"{cat}: {val_fmt} (Z: {z:+.2f})<br>"
        
        hovertemplate += "<extra></extra>"
        
        fig.add_trace(go.Scatterpolar(
            r=r_values,
            theta=cat_keys,
            fill='toself',
            name=row["persona"],
            hovertemplate=hovertemplate
        ))

    fig.update_layout(
        polar=dict(
            radialaxis=dict(
                visible=True,
                range=[0, 1],
                tickmode='array',
                tickvals=[0, 0.25, 0.5, 0.75, 1],
                ticktext=["Muy Bajo", "Bajo", "MEDIA", "Alto", "Muy Alto"],
                gridcolor='#444',
                tickfont=dict(size=10, color='gray')
            ),
            bgcolor='rgba(0,0,0,0)'
        ),
        title=dict(text=title, y=0.95),
        showlegend=True,
        height=700,
        margin=dict(t=80, b=40, l=80, r=80),
        legend=dict(x=1, y=1)
    )
    return fig



# ============================================================
# OTRAS FUNCIONES DE GRAFICOS (SIN CAMBIOS)
# ============================================================
def make_fig(df: pd.DataFrame, title: str):
    n = len(df)
    tick_font = max(12, min(22, int(320 / max(1, n))))
    height = int(max(550, min(1000, 35 * max(1, n))) * 0.75)
    fig = px.bar(df, x="value", y="persona", orientation="h", text="value",
                 color="value", color_continuous_scale="Turbo", title=title)

    fig.update_layout(bargap=0.18)
    fig.update_traces(
        textposition="inside", insidetextanchor="middle", marker_line_width=0,
        textfont=dict(color="white", size=max(12, min(20, int(220 / max(1, n)))))
    )
    fig.update_layout(
        autosize=True, height=height + 90,
        margin=dict(l=170, r=50, t=60, b=40),
        xaxis_title="", yaxis=dict(type="category", tickfont=dict(size=tick_font), automargin=True)
    )
    return fig


def make_fig_games(df: pd.DataFrame, title: str):
    n = len(df)
    tick_font = max(12, min(22, int(320 / max(1, n))))
    height = int(max(550, min(1000, 35 * max(1, n))) * 0.75)
    fig = px.bar(df, x="value", y="persona", orientation="h", text="value",
                 color="value", color_continuous_scale="Turbo", title=title,
                 hover_data={"value": False, "tooltip": True})
    fig.update_layout(bargap=0.18)
    fig.update_traces(
        textposition="inside", insidetextanchor="middle", marker_line_width=0,
        textfont=dict(color="white", size=max(12, min(20, int(220 / max(1, n))))),
        customdata=df["tooltip"],
        hovertemplate="<b>%{y}</b><br>Games: %{x}<br>%{customdata}<extra></extra>"
    )
    fig.update_layout(
        autosize=True, height=height + 90,
        margin=dict(l=170, r=50, t=60, b=40),
        xaxis_title="",
        yaxis=dict(type="category", tickfont=dict(size=tick_font), automargin=True)
    )
    return fig


# ============================================================
# GENERAR DATOS PARA GRAFICOS
# ============================================================
def get_chart_data(raw, selected_role: str, min_games: int = 0):
    if selected_role not in raw:
        return None

    role_data = raw[selected_role]

    filtered = {persona: stats for persona, stats in role_data.items() if stats.get("games", 0) >= min_games}

    if not filtered:
        return None

    rows = {
        "winrate": [], "games": [], "damage": [], "damage_taken": [],
        "gold": [], "farm": [], "vision": [], "turret_damage": [],
        "kills": [], "deaths": [], "assists": [], "kill_participation": []
    }

    total_games = sum(stats["games"] for stats in filtered.values())

    for persona, stats in filtered.items():
        games = stats["games"]
        global_pct = (games / total_games * 100) if total_games else 0
        player_total = sum(r.get(persona, {}).get("games", 0) for r in raw.values())

        rows["winrate"].append({"persona": persona, "value": stats.get("winrate", 0)})
        rows["games"].append({
            "persona": persona, "value": games,
            "tooltip": f"{global_pct:.1f}% en global | {(games/player_total*100) if player_total else 0:.1f}% rol"
        })
        rows["damage"].append({"persona": persona, "value": stats.get("avg_damage", 0)})
        rows["damage_taken"].append({"persona": persona, "value": stats.get("avg_damage_taken", 0)})
        rows["gold"].append({"persona": persona, "value": stats.get("avg_gold", 0)})
        rows["farm"].append({"persona": persona, "value": stats.get("avg_farm", 0)})
        rows["vision"].append({"persona": persona, "value": stats.get("avg_vision", 0)})
        rows["turret_damage"].append({"persona": persona, "value": stats.get("avg_turret_damage", 0)})
        rows["kills"].append({"persona": persona, "value": stats.get("avg_kills", 0)})
        rows["deaths"].append({"persona": persona, "value": stats.get("avg_deaths", 0)})
        rows["assists"].append({"persona": persona, "value": stats.get("avg_assists", 0)})
        rows["kill_participation"].append({"persona": persona, "value": stats.get("avg_kill_participation", 0)})

    # Fix: Ensure DataFrames are created even if lists empty (though filtered checks prevent this mostly)
    return {k: pd.DataFrame(v).sort_values("value") if v else pd.DataFrame(columns=["persona", "value"]) for k, v in rows.items()}


# ============================================================
# RENDER PRINCIPAL
# ============================================================
def render(pool_id: str, queue: int, min_friends: int,
           selected_role: str = None, min_games: int = 0,
           start: str | None = None, end: str | None = None):

    data_file = get_data_file(pool_id, queue, min_friends, start, end)
    raw = load_json(data_file)
    if not raw:
        print(f"[ERROR] No se pudieron cargar datos de {data_file}")
        return []

    roles_data = raw.get("roles", {})

    roles = ["TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"]
    if selected_role:
        roles = [selected_role]

    figures = []
    for role in roles:
        data = get_chart_data(roles_data, role, min_games)
        if data is None:
            continue

        figures.append(make_fig(data["winrate"], f"Winrate - {role}"))
        figures.append(make_fig_games(data["games"], f"Partidas Jugadas - {role}"))

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
        
        # Merge dataframes safely
        if not radar_metrics["avg_damage"].empty:
            dfs = [df.rename(columns={"value": name}) for name, df in radar_metrics.items()]
            poly_df = dfs[0]
            for df2 in dfs[1:]:
                poly_df = pd.merge(poly_df, df2, on="persona", how="outer")
            poly_df.fillna(0, inplace=True)

            figures.append(make_polyhedron(poly_df, f"Poliedro de Estadísticas - {role}"))

    return figures


# ============================================================
# GRAFICA 3: COMPARACION CROSS-ROL
# Escala global (como chart_05) + Datos por rol (como chart_10)
# ============================================================
def calculate_global_means_for_cross_role(roles_data):
    """
    Calcula la media global de cada métrica a través de TODOS los roles.
    Esto crea la "escala" para el radar (como en Gráfica 1).
    
    IMPORTANTE: Pondera por número de partidas para calcular media correcta.
    
    Args:
        roles_data: dict con estructura {role: {persona: {stats}}}
    
    Returns:
        dict con las medias globales de cada métrica
    """
    import numpy as np
    
    # Métricas que vamos a calcular
    all_values = {
        "avg_damage": [],
        "avg_damage_taken": [],
        "avg_gold": [],
        "avg_farm": [],
        "avg_vision": [],
        "avg_turret_damage": [],
        "avg_kills": [],
        "avg_deaths": [],
    }
    
    # Recopilar TODOS los valores ponderados por número de partidas
    for role, personas in roles_data.items():
        for persona, stats in personas.items():
            games = stats.get("games", 0)
            if games == 0:
                continue
            
            # PONDERAR: Repetir el valor promedio por el número de partidas
            # Esto da la media correcta global
            for metric in all_values.keys():
                value = stats.get(metric, 0)
                # Añadir el valor repetido 'games' veces
                all_values[metric].extend([value] * games)
    
    # Calcular la media global
    global_means = {}
    for metric, values in all_values.items():
        if values:
            global_means[metric] = np.mean(values)
        else:
            global_means[metric] = 0
    
    return global_means


def make_cross_role_comparison_polyhedron(
    roles_data,
    player_role_selections,
    global_means,
    title: str
):
    """
    Crea un poliedro para comparar jugadores en DIFERENTES roles.
    
    - Escala (bajo/medio/alto): basada en global_means (media global)
    - Datos de cada jugador: de roles_data (stats específicas del rol)
    
    Args:
        roles_data: dict con estructura {role: {persona: {stats}}}
        player_role_selections: lista de tuplas [(persona, role), ...]
            Ejemplo: [("Fran", "MIDDLE"), ("Eduardo", "UTILITY")]
        global_means: dict con medias globales
        title: título del gráfico
    
    Returns:
        figura de Plotly
    """
    fig = go.Figure()
    
    categories = {
        "Daño Medio": "avg_damage",
        "Daño Recibido": "avg_damage_taken",
        "Oro Medio": "avg_gold",
        "Farm Medio": "avg_farm",
        "Visión Media": "avg_vision",
        "Daño a Torretas": "avg_turret_damage",
        "Asesinatos": "avg_kills",
        "Muertes": "avg_deaths",
    }
    
    # Métricas donde menos es mejor (se invertirán)
    inverse_metrics = {"avg_deaths", "avg_damage_taken"}
    
    # Calcular min y max globales para normalización
    all_values = {metric: [] for metric in categories.values()}
    for role, personas in roles_data.items():
        for persona, stats in personas.items():
            for metric in categories.values():
                all_values[metric].append(stats.get(metric, 0))
    
    min_max = {}
    for metric, values in all_values.items():
        if values:
            min_max[metric] = (min(values), max(values))
        else:
            min_max[metric] = (0, 1)
    
    # Para cada selección (persona, rol)
    for persona, role in player_role_selections:
        # Obtener las estadísticas de esa persona en ese rol específico
        if role not in roles_data:
            continue
        if persona not in roles_data[role]:
            continue
        
        player_stats = roles_data[role][persona]
        
        # Normalizar usando min-max global
        normalized_values = []
        tooltip_lines = []
        
        for cat, metric in categories.items():
            real_value = player_stats.get(metric, 0)
            global_mean = global_means.get(metric, 0)
            
            mn, mx = min_max[metric]
            
            # Normalización min-max con escala 0.2-1.0
            if mx - mn > 0:
                if metric in inverse_metrics:
                    # Invertir: valor bajo -> score alto
                    # Si valor = min -> (mx-mn)/(mx-mn) = 1 -> score 1.0
                    # Si valor = max -> (mx-mx)/(mx-mn) = 0 -> score 0.2
                    norm = 0.2 + 0.8 * (mx - real_value) / (mx - mn)
                else:
                    # Normal: valor alto -> score alto
                    norm = 0.2 + 0.8 * (real_value - mn) / (mx - mn)
            else:
                norm = 0.5
            
            # Clamp entre 0 y 1
            norm = max(0.0, min(1.0, norm))
            
            normalized_values.append(norm)
            
            # Tooltip
            diff = real_value - global_mean
            diff_str = f"+{diff:.2f}" if diff >= 0 else f"{diff:.2f}"
            tooltip_lines.append(
                f"{cat}: {real_value:.2f} (media global: {global_mean:.2f}, {diff_str})"
            )
        
        # Añadir trace al gráfico
        fig.add_trace(go.Scatterpolar(
            r=normalized_values,
            theta=list(categories.keys()),
            fill='toself',
            name=f"{persona} ({role})",
            hovertemplate=(
                f"<b>{persona} ({role})</b><br>" +
                "<br>".join(tooltip_lines) +
                "<extra></extra>"
            )
        ))
    
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 1.05])),
        title=title,
        showlegend=True,
        height=700
    )
    
    return fig


def render_cross_role(
    pool_id: str,
    queue: int,
    min_friends: int,
    player_role_selections: list,
    start: str | None = None,
    end: str | None = None
):
    """
    Renderiza el poliedro de comparación cross-rol (Gráfica 3).
    """
    data_file = get_data_file(pool_id, queue, min_friends, start, end)
    raw = load_json(data_file)
    
    if not raw:
        print(f"[ERROR] No se pudieron cargar datos de {data_file}")
        return []
    
    roles_data = raw.get("roles", {})
    
    if not player_role_selections:
        return []
    
    # Calcular medias globales (para la escala)
    global_means = calculate_global_means_for_cross_role(roles_data)
    
    # Crear el poliedro
    fig = make_cross_role_comparison_polyhedron(
        roles_data,
        player_role_selections,
        global_means,
        "Comparación Cross-Rol (Escala Global, Datos por Rol)"
    )
    
    return [fig]
