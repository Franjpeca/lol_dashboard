"""
dashboard/pages/stats_jugador.py
Sección: Estadísticas de Jugador
"""
import streamlit as st

from dashboard.db import get_player_performance_stats, get_champion_stats_by_role
from dashboard.theme import make_hbar, CHART_SCALE

# Mapeo de labels del desplegable → clave interna
_ROL_OPTIONS = {
    "Todos los roles":   "Todos",
    "Top":               "TOP",
    "Jungla":            "JUNGLE",
    "Mid":               "MID",
    "ADC":               "ADC",
    "Support":           "SUPPORT",
}


def render(pool_id: str, queue_id: int, min_friends: int):
    st.header("Estadísticas de jugador")

    # ── Filtro de rol ──────────────────────────────────────────────────────────
    rol_label = st.selectbox("Filtrar por rol", list(_ROL_OPTIONS.keys()))
    position  = _ROL_OPTIONS[rol_label]

    # ── Datos ──────────────────────────────────────────────────────────────────
    df = get_player_performance_stats(pool_id, queue_id, min_friends, position)
    if df.empty:
        st.warning("Sin datos disponibles para este filtro.")
        return

    # ── Filtro de partidas mínimas ─────────────────────────────────────────────
    max_games = int(df["games"].max())
    min_games = int(df["games"].min())
    umbral = st.slider(
        "Partidas mínimas para aparecer en las gráficas",
        min_value=min_games,
        max_value=max_games,
        value=min_games,
        step=1,
        key="min_games_slider",
        help="Filtra jugadores que tengan menos partidas que el valor indicado en el rol seleccionado."
    )
    df = df[df["games"] >= umbral]
    if df.empty:
        st.info(f"Ningún jugador tiene {umbral}+ partidas con el filtro actual.")
        return

    # ── Row 1: Partidas y Winrate ──────────────────────────────────────────────
    st.subheader("General")
    c_gen1, c_gen2 = st.columns(2)

    with c_gen1:
        # Igual que en winrate.py: Winrate chart
        # Ordenado por winrate · color = nº partidas (sqrt)
        df_win = df.sort_values("winrate", ascending=True)
        fig = make_hbar(
            df_win, x="winrate", y="persona",
            title="Winrate (%)",
            color_scale="Turbo", text_fmt=":.1f", xrange=[0, 100],
            color_col="games", hover_col="games",
            color_transform="sqrt", show_colorbar=True,
            colorbar_title="Partidas"
        )
        fig.add_vline(x=50, line_dash="dash", line_color="#555")
        st.plotly_chart(fig, use_container_width=True, theme=None)

    with c_gen2:
        # Igual que en winrate.py: Games chart
        # Ordenado por nº partidas · color = winrate [40, 57]
        df_games = df.sort_values("games", ascending=True)
        fig = make_hbar(
            df_games, x="games", y="persona",
            title="Partidas jugadas",
            color_scale="Turbo", text_fmt=":.0f",
            color_col="winrate", hover_col="winrate",
            color_range=[40, 57],
            show_colorbar=True, colorbar_title="Winrate (%)"
        )
        st.plotly_chart(fig, use_container_width=True, theme=None)

    # ── Row 2: Damage/min · Gold/min · Vision/min ──────────────────────────────
    st.subheader("Rendimiento por minuto")
    c1, c2, c3 = st.columns(3)

    with c1:
        fig = make_hbar(
            df.sort_values("avg_dmg_per_min", ascending=True),
            x="avg_dmg_per_min", y="persona",
            title="Daño por minuto",
            color_scale=CHART_SCALE, text_fmt=":.1f",
            color_col="avg_dmg_per_min", hover_col="winrate",
        )
        st.plotly_chart(fig, use_container_width=True, theme=None)

    with c2:
        fig = make_hbar(
            df.sort_values("avg_gold_per_min", ascending=True),
            x="avg_gold_per_min", y="persona",
            title="Oro por minuto",
            color_scale=CHART_SCALE, text_fmt=":.1f",
            color_col="avg_gold_per_min", hover_col="winrate",
        )
        st.plotly_chart(fig, use_container_width=True, theme=None)

    with c3:
        fig = make_hbar(
            df.sort_values("avg_vision_per_min", ascending=True),
            x="avg_vision_per_min", y="persona",
            title="Visión por minuto",
            color_scale=CHART_SCALE, text_fmt=":.2f",
            color_col="avg_vision_per_min", hover_col="winrate",
        )
        st.plotly_chart(fig, use_container_width=True, theme=None)

    # ── Row 2: KDA · Kill Participation · Deaths ───────────────────────────────
    st.subheader("Kda & participación")
    c4, c5, c6 = st.columns(3)

    with c4:
        fig = make_hbar(
            df.sort_values("avg_kda", ascending=True),
            x="avg_kda", y="persona",
            title="Kda promedio",
            color_scale=CHART_SCALE, text_fmt=":.2f",
            color_col="avg_kda", hover_col="winrate",
        )
        st.plotly_chart(fig, use_container_width=True, theme=None)

    with c5:
        fig = make_hbar(
            df.sort_values("avg_kill_participation", ascending=True),
            x="avg_kill_participation", y="persona",
            title="Participación en muertes (%)",
            color_scale=CHART_SCALE, text_fmt=":.1f",
            color_col="avg_kill_participation", hover_col="winrate",
        )
        st.plotly_chart(fig, use_container_width=True, theme=None)

    with c6:
        # Deaths: menos = mejor → inversa (Turbo_r: menor valor → rojo)
        fig = make_hbar(
            df.sort_values("avg_deaths", ascending=False),
            x="avg_deaths", y="persona",
            title="Muertes promedio",
            color_scale=CHART_SCALE + "_r", text_fmt=":.2f",
            color_col="avg_deaths", hover_col="winrate",
        )
        st.plotly_chart(fig, use_container_width=True, theme=None)

    # ── Campeones por persona y rol ────────────────────────────────────────────
    st.markdown("---")
    st.subheader("Campeones por persona y rol")

    # Selectores: persona y rol independientes de los filtros superiores
    personas_all = sorted(df["persona"].tolist())

    cc1, cc2 = st.columns(2)
    with cc1:
        champ_persona = st.selectbox(
            "Jugador", ["Todos"] + personas_all,
            key="champ_persona_sel"
        )
    with cc2:
        champ_rol_label = st.selectbox(
            "Rol", list(_ROL_OPTIONS.keys()),
            key="champ_rol_sel"
        )
    champ_position = _ROL_OPTIONS[champ_rol_label]

    champ_df = get_champion_stats_by_role(pool_id, queue_id, min_friends, champ_persona, champ_position)

    if champ_df.empty:
        st.info("Sin datos de campeones para este filtro.")
    else:
        # ── Slider de partidas mínimas (campeones) ────────────────────────────
        champ_max = int(champ_df["total_matches"].max())
        champ_min = int(champ_df["total_matches"].min())
        champ_umbral = st.slider(
            "Partidas mínimas por campeón para aparecer",
            min_value=champ_min,
            max_value=champ_max,
            value=max(champ_min, min(30, champ_max)),
            step=1,
            key="champ_min_games_slider",
            help="Oculta los campeones jugados menos veces que el umbral indicado."
        )
        champ_df = champ_df[champ_df["total_matches"] >= champ_umbral]

        if champ_df.empty:
            st.info(f"Ningún campeón tiene {champ_umbral}+ partidas con el filtro actual.")
        else:
            # Etiqueta del eje Y: incluir persona si se muestran varios jugadores
            if champ_persona == "Todos":
                champ_df = champ_df.copy()
                champ_df["label"] = champ_df.apply(
                    lambda r: f"{r['persona']} - {r['champion_name']}", axis=1
                )
            else:
                champ_df = champ_df.copy()
                champ_df["label"] = champ_df["champion_name"]

            auto_h = max(500, min(3000, 38 * len(champ_df)))

            # ── Dos gráficas: igual que la sección General de jugadores ────────
            gc1, gc2 = st.columns(2)

            with gc1:
                # Winrate chart: ordenado por winrate, coloreado por partidas (sqrt)
                df_win_c = champ_df.sort_values("winrate", ascending=True)
                fig = make_hbar(
                    df_win_c, x="winrate", y="label",
                    title=f"Winrate (%) - {champ_persona} / {champ_rol_label}",
                    color_scale="Turbo", text_fmt=":.1f", xrange=[0, 100],
                    height=auto_h,
                    color_col="total_matches", hover_col="total_matches",
                    color_transform="sqrt", show_colorbar=True,
                    colorbar_title="Partidas"
                )
                fig.add_vline(x=50, line_dash="dash", line_color="#555")
                st.plotly_chart(fig, use_container_width=True, theme=None)

            with gc2:
                # Partidas chart: ordenado por partidas, coloreado por winrate [30,65]
                df_games_c = champ_df.sort_values("total_matches", ascending=True)
                fig = make_hbar(
                    df_games_c, x="total_matches", y="label",
                    title=f"Partidas jugadas - {champ_persona} / {champ_rol_label}",
                    color_scale="Turbo", text_fmt=":.0f",
                    height=auto_h,
                    color_col="winrate", hover_col="winrate",
                    color_range=[30, 65],
                    show_colorbar=True, colorbar_title="Winrate (%)"
                )
                st.plotly_chart(fig, use_container_width=True, theme=None)


