"""
dashboard/pages/records.py
Sección: Récords y Rachas
"""
import streamlit as st

from dashboard.db import get_streaks_by_role
from dashboard.theme import make_hbar, CHART_SCALE

# Mapeo de labels del desplegable → clave interna (igual que stats_jugador)
_ROL_OPTIONS = {
    "Todos los roles": "Todos",
    "Top":             "TOP",
    "Jungla":          "JUNGLE",
    "Mid":             "MID",
    "ADC":             "ADC",
    "Support":         "SUPPORT",
}


def render(pool_id: str, queue_id: int, min_friends: int):
    st.header("Récords y rachas")

    # ── Filtro de rol ──────────────────────────────────────────────────────────
    rol_label = st.selectbox("Filtrar por rol", list(_ROL_OPTIONS.keys()), key="records_rol_sel")
    position  = _ROL_OPTIONS[rol_label]

    str_df = get_streaks_by_role(pool_id, queue_id, min_friends, position)

    # ── Rachas ────────────────────────────────────────────────────────────────
    st.subheader("Rachas históricas")
    if not str_df.empty:
        c1, c2 = st.columns(2)
        with c1:
            fig = make_hbar(str_df.sort_values("max_win_streak", ascending=True),
                        x="max_win_streak", y="persona",
                        title="Mayor racha de victorias", color_scale=CHART_SCALE,
                        text_fmt=":.0f")
            st.plotly_chart(fig, use_container_width=True, theme=None)
        with c2:
            fig = make_hbar(str_df.sort_values("max_lose_streak", ascending=True),
                        x="max_lose_streak", y="persona",
                        title="Mayor racha de derrotas", color_scale=CHART_SCALE,
                        text_fmt=":.0f")
            st.plotly_chart(fig, use_container_width=True, theme=None)
    else:
        st.info("Sin datos de rachas para este filtro.")

    # ── Récords personales ────────────────────────────────────────────────────
    st.subheader("Récords personales")
    
    stat_map = {
        "max_kills":        "Kills máximas",
        "max_deaths":       "Muertes máximas",
        "max_assists":      "Asistencias máximas",
        "max_vision_score": "Puntuación de visión máxima",
        "max_cs":           "Cs máximo",
        "max_damage_dealt": "Daño más alto",
        "max_gold":         "Oro más alto",
    }
    
    selected_stat = st.selectbox("Récord a mostrar", list(stat_map.keys()),
                                 format_func=lambda k: stat_map[k],
                                 key="records_stat_sel")
    st.markdown("Haz doble click para copiar el ID de la partida")

    # Mapeo explícito de clave UI -> columna real en player_performances
    # (evita errores como gold->gold_earned o cs->cs_total)
    stat_col_map = {
        "max_kills": "kills",
        "max_deaths": "deaths",
        "max_assists": "assists",
        "max_vision_score": "vision_score",
        "max_cs": "cs_total",
        "max_damage_dealt": "damage_dealt",
        "max_gold": "gold_earned",
    }
    stat_col_internal = stat_col_map[selected_stat]
    
    # Importar lazy o globalmente en la función 
    from dashboard.db import get_records_by_stat
    rec_df = get_records_by_stat(pool_id, queue_id, min_friends, stat_col_internal, position)

    if not rec_df.empty:
        rec_df = rec_df.sort_values("record_value", ascending=True)
        fig = make_hbar(rec_df,
                        x="record_value", y="persona",
                        title=stat_map[selected_stat], color_scale=CHART_SCALE)
        
        # Enganchar el id de la partida al gráfico para el tooltip e interactividad
        fig.update_traces(
            customdata=rec_df[["record_match_id"]].values, # Pasamos los IDs a cada barra
            hovertemplate="<b>%{y}</b><br>Récord: %{x}<br>ID Partida: %{customdata[0]}<br><i>(Haz click en la barra para copiar ID)</i><extra></extra>"
        )

        event = st.plotly_chart(fig, use_container_width=True, theme=None, on_select="rerun", selection_mode="points")
        
        if event and event.get("selection") and event["selection"].get("points"):
            pt = event["selection"]["points"][0]
            match_id = pt["customdata"][0]
            st.success(f"**Partida seleccionada:** {pt['y']} hizo {pt['x']} {stat_map[selected_stat].lower()}")
            st.code(match_id, language="text") # El st.code ya provee un botón nativo para Copiar al Portapapeles
            
    else:
        st.info("Sin datos de récords para este filtro.")

