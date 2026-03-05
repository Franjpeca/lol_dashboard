"""
dashboard/pages/indices.py
Sección: Índices – Índice de Ego
"""
import streamlit as st
import plotly.graph_objects as go
import plotly.express as px

from dashboard.db import get_ego_score, get_troll_index
from dashboard.theme import make_hbar, CHART_SCALE, GOLD, RED, GREEN, BG, BORDER, TEXT


# Paleta base para las barras de desglose
_DMG_COLOR  = "#e76f51"   # naranja-rojo  → daño
_GOLD_COLOR = "#f4d03f"   # dorado        → oro
_ASST_COLOR = "#52b3d9"   # azul          → asistencias


def render(pool_id: str, queue_id: int, min_friends: int):
    st.header("Índices")

    # ══════════════════════════════════════════════════════════════════════════
    # Ego Index
    # ══════════════════════════════════════════════════════════════════════════
    st.subheader("Índice de ego")
    st.caption(
        "**ego = (% daño del equipo + % oro del equipo) − % asistencias del equipo**  "
        "- Cuanto más alto el ego, más el jugador prefiere hacer daño y acumular oro "
        "que asistir a sus compañeros. Cuanto más bajo, más equipo."
    )

    df = get_ego_score(pool_id, queue_id, min_friends)

    if df.empty:
        st.info("Sin datos para este filtro.")
    else:
        df_sorted = df.sort_values("ego_score", ascending=False)
        max_abs = float(df_sorted["ego_score"].abs().max()) or 1.0

        bar_colors = [
            f"rgba({max(0, int(255*(v/max_abs)))}, {max(0, int(255*(1-v/max_abs)))}, 80, 0.85)"
            if v >= 0
            else f"rgba(80, {max(0, int(255*(1-abs(v)/max_abs)))}, 255, 0.85)"
            for v in df_sorted["ego_score"].astype(float)
        ]

        fig_ego = go.Figure()
        fig_ego.add_trace(go.Bar(
            x=df_sorted["ego_score"],
            y=df_sorted["persona"],
            orientation="h",
            marker_color=bar_colors,
            text=df_sorted["ego_score"].apply(lambda v: f"{v:+.1f}"),
            textposition="outside",
            textfont=dict(color="white", size=12),
            hovertemplate="<b>%{y}</b><br>Ego score: <b>%{x:+.2f}</b><extra></extra>",
            width=0.6,
        ))
        fig_ego.update_layout(
            paper_bgcolor=BG, plot_bgcolor=BG,
            font=dict(color=TEXT, family="Inter, sans-serif"),
            title=dict(text="Ego score (mayor = más egoísta)", font=dict(color=GOLD, size=15)),
            height=max(300, len(df_sorted) * 55 + 80),
            margin=dict(t=60, b=40, l=120, r=80),
            xaxis=dict(showgrid=True, gridcolor=BORDER, zeroline=True,
                       zerolinecolor="rgba(255,255,255,0.3)", zerolinewidth=1.5),
            yaxis=dict(showgrid=False, autorange="reversed"),
        )
        st.plotly_chart(fig_ego, use_container_width=True, theme=None)

        st.markdown("**Variables base**")
        c1, c2, c3 = st.columns(3)
        with c1:
            fig = make_hbar(df.sort_values("avg_dpm", ascending=True),
                            x="avg_dpm", y="persona", title="Daño por minuto",
                            color_scale=CHART_SCALE, text_fmt=":.0f")
            st.plotly_chart(fig, use_container_width=True, theme=None)
        with c2:
            fig = make_hbar(df.sort_values("avg_gpm", ascending=True),
                            x="avg_gpm", y="persona", title="Oro por minuto",
                            color_scale=CHART_SCALE, text_fmt=":.0f")
            st.plotly_chart(fig, use_container_width=True, theme=None)
        with c3:
            fig = make_hbar(df.sort_values("avg_kp", ascending=True),
                            x="avg_kp", y="persona", title="Kill Participation (%)",
                            color_scale=CHART_SCALE, text_fmt=":.1f", xrange=[0, 100])
            st.plotly_chart(fig, use_container_width=True, theme=None)

        with st.expander("Ver tabla completa - Ego"):
            show_df = df_sorted.rename(columns={
                "persona": "Jugador", "avg_damage_share": "% Daño E.",
                "avg_gold_share": "% Oro E.", "avg_assist_share": "% Asist. E.",
                "avg_kp": "KP%", "ego_score": "Ego score",
                "avg_dpm": "DPM", "avg_gpm": "GPM", "partidas": "Partidas",
            })
            st.dataframe(show_df.set_index("Jugador"), use_container_width=True)

    st.divider()

    # ══════════════════════════════════════════════════════════════════════════
    # Troll Index
    # ══════════════════════════════════════════════════════════════════════════
    st.subheader("Troll Index")
    st.caption(
        "**troll_index = 0.5 × afk_rate + 0.3 × grief_rate + 0.2 × remake_rate**  \n"
        "Detecta partidas sospechosas: AFK (DPM<50, GPM<100, CS<20), "
        "grief/inting (≥10 muertes, KP<20%, daño<10%) y remakes (<5 min). "
        "El índice es en %, mayor = más troll"
    )

    tdf = get_troll_index(pool_id, queue_id, min_friends)

    if tdf.empty:
        st.info("Sin datos para el Troll Index.")
        return

    tdf_sorted = tdf.sort_values("troll_index", ascending=False)

    # Gráfico principal - Troll Index
    troll_colors = [
        f"rgba({min(255, int(v * 5))}, {max(0, 255 - int(v * 5))}, 60, 0.85)"
        for v in tdf_sorted["troll_index"].astype(float)
    ]
    fig_troll = go.Figure()
    fig_troll.add_trace(go.Bar(
        x=tdf_sorted["troll_index"],
        y=tdf_sorted["persona"],
        orientation="h",
        marker_color=troll_colors,
        text=tdf_sorted["troll_index"].apply(lambda v: f"{v:.1f}%"),
        textposition="outside",
        textfont=dict(color="white", size=12),
        hovertemplate=(
            "<b>%{y}</b><br>Troll Index: <b>%{x:.2f}%</b><extra></extra>"
        ),
        width=0.6,
    ))
    fig_troll.update_layout(
        paper_bgcolor=BG, plot_bgcolor=BG,
        font=dict(color=TEXT, family="Inter, sans-serif"),
        title=dict(text="Troll Index global", font=dict(color=GOLD, size=15)),
        height=max(300, len(tdf_sorted) * 55 + 80),
        margin=dict(t=60, b=40, l=120, r=80),
        xaxis=dict(showgrid=True, gridcolor=BORDER),
        yaxis=dict(showgrid=False, autorange="reversed"),
    )
    st.plotly_chart(fig_troll, use_container_width=True, theme=None)

    # Barras de desglose
    c1, c2, c3 = st.columns(3)
    with c1:
        fig = make_hbar(tdf.sort_values("afk_games", ascending=True),
                        x="afk_games", y="persona", title="Partidas AFK detectadas",
                        color_scale=CHART_SCALE, text_fmt=":.0f")
        st.plotly_chart(fig, use_container_width=True, theme=None)
    with c2:
        fig = make_hbar(tdf.sort_values("remake_games", ascending=True),
                        x="remake_games", y="persona", title="Remakes (<5 min)",
                        color_scale=CHART_SCALE, text_fmt=":.0f")
        st.plotly_chart(fig, use_container_width=True, theme=None)
    with c3:
        fig = make_hbar(tdf.sort_values("grief_games", ascending=True),
                        x="grief_games", y="persona", title="Partidas grief/inting",
                        color_scale=CHART_SCALE, text_fmt=":.0f")
        st.plotly_chart(fig, use_container_width=True, theme=None)

    with st.expander("Ver tabla completa - Troll"):
        show_tdf = tdf_sorted.rename(columns={
            "persona": "Jugador", "total_games": "Partidas",
            "afk_games": "AFK", "grief_games": "Grief",
            "remake_games": "Remakes", "afk_rate": "AFK%",
            "grief_rate": "Grief%", "remake_rate": "Remake%",
            "troll_index": "Troll Index",
        })
        st.dataframe(show_tdf.set_index("Jugador"), use_container_width=True)

