"""
dashboard/pages/winrate.py
Sección: Winrate y Partidas
"""
import streamlit as st
import plotly.graph_objects as go
import pandas as pd
from dashboard.db import (
    get_winrate_by_persona,
    get_community_champions, get_enemy_champions,
    get_matches_per_day, get_matches_per_day_persona,
    get_community_overall_stats, get_top_outsider_allies
)
from dashboard.theme import (
    make_hbar, CHART_SCALE,
    BG, BORDER, TEXT, MUTED, GOLD
)


def _vbar(df: pd.DataFrame, date_col: str, count_col: str, title: str) -> go.Figure:
    """Gráfico de barras verticales con gradiente Turbo (max=rojo, min=morado)."""
    import numpy as np

    c_raw = pd.to_numeric(df[count_col], errors="coerce").fillna(0)
    # Rango de color: el máximo siempre es rojo, el 0 es morado
    c_max = float(c_raw.max()) if c_raw.max() > 0 else 1

    bar = go.Bar(
        x=df[date_col].astype(str),
        y=c_raw.tolist(),
        marker=dict(
            color=c_raw.tolist(),
            colorscale="Turbo",
            cmin=0,
            cmax=c_max,
            showscale=True,
            colorbar=dict(
                title=dict(text="Partidas", font=dict(size=10, color=MUTED)),
                thickness=12,
                len=0.7,
                tickfont=dict(size=9, color=MUTED),
            ),
        ),
        hovertemplate="%{x}<br>Partidas: %{y}<extra></extra>",
    )
    fig = go.Figure(data=[bar])
    fig.update_layout(
        paper_bgcolor=BG,
        plot_bgcolor=BG,
        showlegend=False,
        font=dict(color=TEXT, family="Inter, sans-serif"),
        title=dict(text=title, font=dict(color=GOLD, size=14), pad=dict(b=8)),
        height=320,
        margin=dict(l=40, r=80, t=55, b=50),
        xaxis=dict(
            showgrid=False,
            tickfont=dict(size=9),
            color=TEXT,
            title="",
            type="category",
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor=BORDER,
            tickfont=dict(size=10),
            color=TEXT,
            title="Partidas",
            rangemode="tozero",
        ),
        bargap=0.15,
        hoverlabel=dict(bgcolor="#111", font_color=TEXT, bordercolor=BORDER),
    )
    return fig


def render(pool_id: str, queue_id: int, min_friends: int):
    st.header("Winrate y partidas")

    overall = get_community_overall_stats(pool_id, queue_id, min_friends)
    st.markdown(
        f"<h3 style='text-align: center; color: #c9aa71; margin-bottom: 2rem;'>"
        f"Partidas jugadas: <span style='color: white;'>{overall['matches']}</span> &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; "
        f"Winrate: <span style='color: white;'>{overall['winrate']}%</span></h3>",
        unsafe_allow_html=True
    )


    # ── Rendimiento general ───────────────────────────────────────────────────
    st.subheader("Rendimiento general")
    c1, c2 = st.columns(2)

    persona_df = get_winrate_by_persona(pool_id, queue_id, min_friends)
    if persona_df is not None and not persona_df.empty:
        with c1:
            # Ordenado por winrate · color = nº partidas · tooltip = nº partidas
            df = persona_df.sort_values("winrate", ascending=True)
            # Winrate chart: color by games (sqrt = gradiente suave) + Turbo
            fig = make_hbar(df, x="winrate", y="persona",
                            title="Winrate (%)", color_scale="Turbo",
                            text_fmt=":.1f", xrange=[0, 100],
                            color_col="total_matches", hover_col="total_matches",
                            color_transform="sqrt", show_colorbar=True,
                            colorbar_title="Partidas")
            fig.add_vline(x=50, line_dash="dash", line_color="#555")
            st.plotly_chart(fig, theme=None, use_container_width=True)

        with c2:
            # Ordenado por nº partidas · color = winrate · tooltip = winrate
            df = persona_df.sort_values("total_matches", ascending=True)
            # Games chart: color por winrate (Turbo, 57%=rojo, 40%=morado)
            fig = make_hbar(df, x="total_matches", y="persona",
                            title="Partidas jugadas", color_scale="Turbo",
                            text_fmt=":.0f",
                            color_col="winrate", hover_col="winrate",
                            color_range=[40, 57],
                            show_colorbar=True, colorbar_title="Winrate (%)")
            st.plotly_chart(fig, theme=None, use_container_width=True)
    else:
        st.warning("Sin datos de jugadores disponibles.")

    st.markdown("---")

    # ── Pickrate y rendimiento de campeones ───────────────────────────────────
    st.subheader("Pickrate y rendimiento de campeones")

    order_by = st.radio("Ordenar gráficos por:",
                        ["Número de partidas", "Winrate"],
                        horizontal=True)

    if order_by == "Número de partidas":
        sort_col  = "games"
        color_col = "winrate"
        text_fmt  = ":.0f"
    else:
        sort_col  = "winrate"
        color_col = "games"
        text_fmt  = ":.1f"

    c3, c4 = st.columns(2)

    def _champ_chart(df_raw, title, col):
        if df_raw.empty:
            col.info("Sin datos.")
            return
        df = df_raw.sort_values(sort_col, ascending=True)
        if color_col == "winrate":
            # Ordenado por partidas, coloreado por winrate → Turbo [30-65%], 65=rojo
            fig = make_hbar(df, x=sort_col, y="champion_name",
                            title=title, color_scale="Turbo",
                            text_fmt=text_fmt, height=1200,
                            color_col=color_col, hover_col=color_col,
                            color_range=[30, 65],
                            show_colorbar=True, colorbar_title="Winrate %")
        else:
            # Ordenado por winrate, coloreado por partidas → Turbo sqrt (gradiente suave)
            fig = make_hbar(df, x=sort_col, y="champion_name",
                            title=title, color_scale="Turbo",
                            text_fmt=text_fmt, height=1200,
                            color_col=color_col, hover_col=color_col,
                            color_transform="sqrt", show_colorbar=True,
                            colorbar_title="Partidas")
        if sort_col == "winrate":
            fig.add_vline(x=50, line_dash="dash", line_color="#555")
            fig.update_layout(xaxis=dict(range=[0, 100]))
        col.plotly_chart(fig, theme=None, use_container_width=True)

    _champ_chart(get_community_champions(pool_id, queue_id, min_friends),
                 "Nuestros campeones más jugados", c3)
    _champ_chart(get_enemy_champions(pool_id, queue_id, min_friends),
                 "Campeones jugados por el enemigo", c4)

    st.markdown("---")

    # ── Frecuencia de partidas ───────────────────────────────────────────────
    st.subheader("Frecuencia de partidas")

    def _fill_dates(df: pd.DataFrame, global_min: pd.Timestamp, global_max: pd.Timestamp, day_col: str = "day") -> pd.DataFrame:
        """Rellena los días sin partidas con 0 en el rango global para alinear los gráficos."""
        df = df.copy()
        df[day_col] = pd.to_datetime(df[day_col])
        full_range = pd.date_range(global_min, global_max, freq="D")
        df = df.set_index(day_col).reindex(full_range, fill_value=0).reset_index()
        df.rename(columns={"index": day_col}, inplace=True)
        df[day_col] = df[day_col].dt.strftime("%Y-%m-%d")
        return df

    global_df = get_matches_per_day(pool_id, queue_id, min_friends)
    g_min, g_max = None, None
    if not global_df.empty:
        g_min = pd.to_datetime(global_df["day"]).min()
        g_max = pd.to_datetime(global_df["day"]).max()
        st.markdown("**Frecuencia global de partidas por día**")
        
        fig_glob = _vbar(_fill_dates(global_df, g_min, g_max), date_col="day", count_col="matches",
                         title="Partidas por día - Comunidad completa")
        st.plotly_chart(fig_glob, use_container_width=True, theme=None)
    else:
        st.info("Sin datos de frecuencia global.")

    st.markdown("<div style='margin-bottom: 3rem;'></div>", unsafe_allow_html=True)

    # Por persona
    persona_freq_df = get_matches_per_day_persona(pool_id, queue_id, min_friends)
    if not persona_freq_df.empty and g_min is not None:
        st.markdown("**Frecuencia por jugador**")
        personas = persona_freq_df["persona"].dropna().unique().tolist()
        personas.sort()
        p_sel = st.selectbox("Seleccionar jugador", personas, key="winrate_freq_persona_sel")
        df_p = persona_freq_df[persona_freq_df["persona"] == p_sel]
        
        fig_p = _vbar(_fill_dates(df_p, g_min, g_max), date_col="day", count_col="matches",
                       title=f"Partidas por día - {p_sel}")
        st.plotly_chart(fig_p, use_container_width=True, theme=None)

    st.markdown("---")

    # ── Aliados frecuentes (fuera de la lista) ──────────────────────────────
    st.subheader("Top aliados frecuentes (fuera de la lista)")
    st.info("Jugadores que no están en tu mapa de cuentas pero con los que has coincidido en tu equipo.")
    
    outsider_df = get_top_outsider_allies(pool_id, queue_id, min_friends)
    if not outsider_df.empty:
        total_outsider_games = int(outsider_df["games"].sum())
        st.markdown(f"**Suma de partidas con otra gente: {total_outsider_games}**")

        # Ordenado por nº partidas
        df_vis = outsider_df.sort_values("games", ascending=True)
        dynamic_height = max(500, len(df_vis) * 25)
        
        fig_out = make_hbar(df_vis, x="games", y="summoner",
                            title="Veces coincidido", color_scale="Turbo",
                            text_fmt=":.0f", height=dynamic_height,
                            color_col="winrate", hover_col="winrate",
                            color_range=[30, 70],
                            show_colorbar=True, colorbar_title="Winrate %")
        st.plotly_chart(fig_out, theme=None, use_container_width=True)
    else:
        st.info("No se han encontrado aliados fuera de la lista para los filtros seleccionados.")

