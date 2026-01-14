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



def calculate_z_scores_and_normalize(df: pd.DataFrame, min_games: int):
    """
    Realiza el cálculo estadístico:
    1. Filtra por mínimo de partidas.
    2. Calcula media y desviación estándar del GRUPO (solo filtrados).
    3. Calcula Z-score por jugador.
    4. Normaliza a escala radar 0-1 (0.5 = media).
    """
    # 0. Definir columnas de métricas
    metrics = {
        "kda": "avg_kda",
        "kills": "avg_kills",
        "deaths": "avg_deaths",
        "assists": "avg_assists",
        "gold": "avg_gold",
        "damage_dealt": "avg_damage_dealt",
        "damage_taken": "avg_damage_taken",
        "vision": "avg_vision_score"
    }
    
    # 1. Filtrar jugadores
    # Aseguramos que la columna games exista (si no, asumimos 1 para no romper, pero metrics_05 deberia tenerla)
    if "games" in df.columns:
        df_filtered = df[df["games"] >= min_games].copy()
    else:
        df_filtered = df.copy()

    if df_filtered.empty:
        return df_filtered, {}

    # Diccionario para guardar referencias (media y std) para tooltips o debug
    references = {}

    # 2. y 3. Calcular Z-scores
    for metric_name, col_name in metrics.items():
        if col_name not in df_filtered.columns:
            continue

        raw_values = df_filtered[col_name]
        mean = raw_values.mean()
        std = raw_values.std()
        
        # Evitar división por cero
        if std == 0:
            std = 1  # Si todos son iguales, z será 0

        references[metric_name] = {"mean": mean, "std": std}

        # Calcular Z - LÓGICA DIRECTA PARA TODO
        # Más valor en cualquier métrica = Más alejado del centro
        z_scores = (raw_values - mean) / std

            
        # 4. Transformar a Radar Value
        # 0.5 + z/4
        # z=0 -> 0.5
        # z=2 -> 1.0 (Top 2% aprox)
        # z=-2 -> 0.0
        
        radar_col = f"radar_{metric_name}"
        z_col = f"z_{metric_name}"
        
        df_filtered[z_col] = z_scores
        # Clamp entre 0 y 1
        df_filtered[radar_col] = (0.5 + z_scores / 4).clip(0, 1)

    return df_filtered, references


def make_polyhedron(df: pd.DataFrame, references: dict, title: str):
    """
    Crea el gráfico de radar estadístico.
    """
    fig = go.Figure()

    categories_map = {
        "KDA": "radar_kda",
        "Asesinatos": "radar_kills",
        "Muertes": "radar_deaths",
        "Asistencias": "radar_assists",
        "Oro": "radar_gold",
        "Daño Infligido": "radar_damage_dealt",
        "Daño Recibido": "radar_damage_taken",
        "Visión": "radar_vision",
    }
    
    original_cols = {
        "KDA": "avg_kda",
        "Asesinatos": "avg_kills",
        "Muertes": "avg_deaths",
        "Asistencias": "avg_assists",
        "Oro": "avg_gold",
        "Daño Infligido": "avg_damage_dealt",
        "Daño Recibido": "avg_damage_taken",
        "Visión": "avg_vision_score",
    }

    # Orden de las categorías para cerrar el ciclo
    cat_keys = list(categories_map.keys())
    
    # 1. Dibujar línea de referencia (Media del grupo = 0.5)
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
    for _, row in df.iterrows():
        r_values = [row.get(col, 0.5) for col in categories_map.values()]
        
        # Tooltip customizado con valores reales y Z-score
        hovertemplate = f"<b>{row['persona']}</b><br><br>"
        for cat, col_radar in categories_map.items():
            orig_col = original_cols[cat]
            val = row.get(orig_col, 0)
            z = row.get(col_radar.replace("radar_", "z_"), 0)
            
            # Formateo bonito según la métrica
            val_fmt = f"{val:.2f}"
            if "gold" in orig_col or "damage" in orig_col:
                val_fmt = f"{val:,.0f}"
            
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
        showlegend=True,
        title=dict(text=title, y=0.95),
        height=700,
        margin=dict(t=80, b=40, l=80, r=80),
        legend=dict(x=1, y=1)
    )
    return fig


# ============================================================
#   FUNCIÓN PARA USAR EN TU FLUJO (sin Dash)
# ============================================================

def render(pool_id: str, queue: int, min_friends: int, start: str | None = None, end: str | None = None):
    """
    Función que retorna las figuras de Plotly para usar en tu flujo existente.
    """
    data_file = get_data_file(pool_id, queue, min_friends, start, end)
    data = load_json(data_file)
    
    if not data:
        print(f"[ERROR] No se pudieron cargar datos de {data_file}")
        return []
    
    # Construir DataFrame crudo
    players_dict = data.get("players", {})
    if not players_dict:
        return []
        
    records = []
    for p, stats in players_dict.items():
        rec = stats.copy()
        rec["persona"] = p
        records.append(rec)
    
    df_raw = pd.DataFrame(records)
    
    # Manejar caso de que no existan las columnas si el JSON es viejo
    if "games" not in df_raw.columns:
        # Fallback si no se ha regenerado metrics_05
        df_raw["games"] = 1 

    # 1. Crear figuras de barras (usamos lógica similar a antes pero directo del DF)
    # Reutilizamos las funciones make_fig existentes, pero necesitamos pasarle DFs individuales
    # o adaptar build_dataframes. Para no romper mucho, adaptamos build_dataframes AQUI MISMO
    # o simplemente pasamos el DF crudo si las funciones auxiliares esperan eso.
    # Las funciones make_kda_fig esperan un dict con DFs ordenados.
    
    # Reconstruimos el dict de dfs para las barras (lógica Visual legacy)
    dfs_bars = {}
    metrics_cols = {
        "kda": "avg_kda",
        "kills": "avg_kills",
        "deaths": "avg_deaths",
        "assists": "avg_assists",
        "gold": "avg_gold",
        "damage_dealt": "avg_damage_dealt",
        "damage_taken": "avg_damage_taken",
        "vision": "avg_vision_score",
    }
    
    for key, col in metrics_cols.items():
        if col in df_raw.columns:
            # Ordenar ascendente para barras horizontales (mejor valor arriba o abajo según gusto,
            # pero el código original usaba sort_values('value')).
            dfs_bars[key] = df_raw[["persona", col]].rename(columns={col: "value"}).sort_values("value")

    figures = [
        make_kda_fig(dfs_bars),
        make_kills_fig(dfs_bars),
        make_deaths_fig(dfs_bars),
        make_assists_fig(dfs_bars),
        make_gold_fig(dfs_bars),
        make_damage_dealt_fig(dfs_bars),
        make_damage_taken_fig(dfs_bars),
        make_vision_fig(dfs_bars),
    ]

    # 2. GENERAR EL NUEVO POLIEDRO ESTADÍSTICO
    # Usamos el min_friends pasado como argumento para filtrar "consistencia"
    # OJO: 'min_friends' en arg suele ser "min amigos en partida", no "min total de partidas".
    # Pero el usuario pidió "Eliminar jugadores que no cumplan el mínimo de partidas configurado".
    # Asumiré un valor razonable por defecto (ej. 3) o si hay alguna config global. 
    # En metrics_10 usamos MIN_GAMES_FOR_RANKING = 3. Usaré 3 como hardcode sensato o 5.
    # Dado que es visualización, filtrar mucho puede dejar el gráfico vacío. 
    # USAMOS 3 COMO BASELINE PARA RANKINGS.
    
    MIN_GAMES_RADAR = 3
    
    df_radar, refs = calculate_z_scores_and_normalize(df_raw, min_games=MIN_GAMES_RADAR)
    
    if not df_radar.empty:
        poly = make_polyhedron(df_radar, refs, "Estadísticas Globales (Desviación vs Media)")
        figures.append(poly)
    else:
        # Si no hay datos suficientes, devolver quizás una fig vacía o nada
        pass

    return figures