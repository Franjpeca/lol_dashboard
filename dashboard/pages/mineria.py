import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import networkx as nx
import itertools
from dashboard.db import (
    get_fiesta_stats, get_dangerous_enemy_comps, get_enemy_heat_data, 
    get_match_anomaly_data, get_apriori_raw_data
)
from dashboard.theme import GOLD, PAPER, TEXT, MUTED, BG, BORDER

def render_fiesta_tab(pool_id, queue_id, min_friends):
    st.markdown("<h3 style='color:"+GOLD+";'>Champions Fiesta</h3>", unsafe_allow_html=True)
    st.markdown(
        f"<p style='color:{MUTED}; margin-bottom:25px;'>"
        "Fiesta Score measures how chaotic a match is based on kills, damage and objective tempo, "
        "using only end-of-game summary data."
        "</p>",
        unsafe_allow_html=True
    )

    col_filter, _ = st.columns([1, 2])
    with col_filter:
        min_games_fiesta = st.slider(
            "Mínimo de partidas por campeón (Fiesta)",
            min_value=1,
            max_value=30,
            value=10,
            step=1,
            key="fiesta_min_games"
        )

    with st.spinner("Calculando niveles de fiesta..."):
        df = get_fiesta_stats(pool_id, queue_id, min_friends, min_games=min_games_fiesta)

    if df.empty:
        st.warning(f"No hay suficientes datos para el análisis de Fiesta (se requiere un mínimo de {min_games_fiesta} partidas por campeón).")
        return

    # Ranking bar chart
    fig_bar = px.bar(
        df.sort_values("avg_fiesta_score", ascending=True),
        y="champion_name",
        x="avg_fiesta_score",
        orientation='h',
        labels={"champion_name": "Campeón", "avg_fiesta_score": "Fiesta Score"},
        color="avg_fiesta_score",
        color_continuous_scale="Turbo"
    )
    fig_bar.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor=PAPER,
        font=dict(family="Inter, sans-serif", color=TEXT),
        height=int(max(400, len(df) * 30)),
        margin=dict(l=150, r=20, t=20, b=40),
        xaxis=dict(showgrid=True, gridcolor="#27272A"),
        yaxis=dict(showgrid=False),
        coloraxis_showscale=False
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    c1, c2 = st.columns([1.5, 1])
    with c1:
        st.markdown("#### Fiesta vs Popularidad")
        fig_scatter = px.scatter(
            df, x="games", y="avg_fiesta_score", size="games", color="fiesta_game_rate",
            text="champion_name", hover_name="champion_name",
            labels={"games": "Games", "avg_fiesta_score": "Fiesta Score", "fiesta_game_rate": "Fiesta Match Rate"},
            color_continuous_scale="Turbo"
        )
        fig_scatter.update_traces(textposition='top center', hovertemplate="<b>%{hovertext}</b><br>Games: %{x}<br>Fiesta Score: %{y:.2f}<br>Fiesta Match Rate: %{marker.color:.1%}<extra></extra>")
        fig_scatter.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor=PAPER, font=dict(family="Inter, sans-serif", color=TEXT), height=600)
        st.plotly_chart(fig_scatter, use_container_width=True)

    with c2:
        st.markdown("#### Fiesta Match Rate")
        df_rate = df.sort_values("fiesta_game_rate", ascending=True)
        fig_rate = px.bar(df_rate, y="champion_name", x="fiesta_game_rate", orientation='h', color="fiesta_game_rate", color_continuous_scale="Turbo")
        fig_rate.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor=PAPER, font=dict(family="Inter, sans-serif", color=TEXT), height=int(max(400, len(df) * 30)), margin=dict(l=120, r=20), xaxis=dict(tickformat=".0%"), coloraxis_showscale=False)
        st.plotly_chart(fig_rate, use_container_width=True)


def render_dangerous_tab(pool_id, queue_id, min_friends):
    st.markdown("<h3 style='color:"+GOLD+";'>Composiciones Enemigas Peligrosas</h3>", unsafe_allow_html=True)
    st.markdown(
        f"<p style='color:{MUTED}; margin-bottom:25px;'>"
        "Identifica qué combinaciones de campeones enemigos son especialmente problemáticas para nuestro grupo, "
        "calculando el Danger Score (tasa de derrota) contra ellas."
        "</p>",
        unsafe_allow_html=True
    )

    f1, f2, f3 = st.columns([1, 1, 2])
    with f1:
        comp_type = st.radio("Tipo de combinación", ["Pares (2)", "Tríos (3)"], horizontal=True)
        comp_size = 2 if "Pares" in comp_type else 3
    with f2:
        min_games_comp = st.slider(
            "Mínimo de partidas vs combo",
            min_value=1, max_value=20, value=5 if comp_size == 2 else 3, step=1
        )

    with st.spinner("Analizando sinergias enemigas..."):
        df_comp = get_dangerous_enemy_comps(pool_id, queue_id, min_friends, min_games=min_games_comp, comp_size=comp_size)

    if df_comp.empty:
        st.warning(f"No hay suficientes datos para combinaciones enemigas de tamaño {comp_size} con {min_games_comp} partidas.")
        return

    # A) Ranking Bar Chart
    st.markdown("#### Ranking de Peligrosidad")
    fig_rank = px.bar(
        df_comp.sort_values("danger_score", ascending=True).tail(20), # Top 20 most dangerous
        y="combination", x="danger_score", orientation='h',
        color="danger_score", color_continuous_scale="Reds",
        labels={"combination": "Combo Enemigo", "danger_score": "Danger Score"},
        custom_data=["games", "wins", "losses", "winrate"]
    )
    fig_rank.update_traces(hovertemplate="<b>%{y}</b><br>Danger Score: %{x:.2f}<br>Games: %{customdata[0]}<br>Wins: %{customdata[1]} (WR: %{customdata[3]:.1%})<extra></extra>")
    fig_rank.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor=PAPER, font=dict(family="Inter, sans-serif", color=TEXT), height=600, margin=dict(l=200), coloraxis_showscale=False)
    st.plotly_chart(fig_rank, use_container_width=True)

    # B) Scatter plot
    st.markdown("#### Frecuencia vs Peligrosidad")
    fig_scat = px.scatter(
        df_comp, x="games", y="winrate", size="games", color="danger_score",
        text="combination", hover_name="combination",
        color_continuous_scale="Reds",
        labels={"games": "Partidas Jugadas", "winrate": "Nuestro Winrate (%)", "danger_score": "Nivel de Peligro"}
    )
    fig_scat.update_traces(textposition='top center', hovertemplate="<b>%{hovertext}</b><br>Games: %{x}<br>WR vs Combo: %{y:.1%}<br>Danger: %{marker.color:.2f}<extra></extra>")
    fig_scat.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor=PAPER, font=dict(family="Inter, sans-serif", color=TEXT), height=600, yaxis=dict(tickformat=".0%", gridcolor=BORDER))
    st.plotly_chart(fig_scat, use_container_width=True)

    # C) Heatmap (solo para pares)
    if comp_size == 2:
        st.markdown("#### Mapa de Sinergia Enemiga")
        st.markdown(f"<p style='color:{MUTED}; font-size:0.9rem;'>Winrate contra pares de los campeones más frecuentes.</p>", unsafe_allow_html=True)
        df_heat = get_enemy_heat_data(pool_id, queue_id, min_friends, top_n=20)
        
        if not df_heat.empty:
            matrix = df_heat.pivot(index="c1", columns="c2", values="winrate")
            fig_heat = px.imshow(
                matrix, color_continuous_scale="RdYlGn",
                labels=dict(color="Winrate"), text_auto=".0%"
            )
            fig_heat.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor=PAPER, height=700, margin=dict(t=50))
            st.plotly_chart(fig_heat, use_container_width=True)


def render_anomalies_tab(pool_id, queue_id, min_friends):
    st.markdown("<h3 style='color:"+GOLD+";'>Detección de Anomalías</h3>", unsafe_allow_html=True)
    st.markdown(
        f"<p style='color:{MUTED}; margin-bottom:25px;'>"
        "Utiliza Isolation Forest para detectar partidas con métricas inusuales (outliers) "
        "y las clasifica según su naturaleza: Stomp, Fiesta, Comeback, etc."
        "</p>",
        unsafe_allow_html=True
    )

    f1, _ = st.columns([1, 2])
    with f1:
        contamination = st.slider(
            "Sensibilidad (Contamination)",
            min_value=0.01, max_value=0.10, value=0.03, step=0.01,
            help="Proporción esperada de anomalías en el dataset."
        )

    with st.spinner("Entrenando modelo de detección..."):
        df_anom = get_match_anomaly_data(pool_id, queue_id, min_friends, contamination=contamination)

    if df_anom.empty:
        st.warning("No hay suficientes partidas para realizar el análisis de anomalías.")
        return

    # 1. Scatter Charts (Redesigned)
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Duración vs Diferencia de Oro")
        st.markdown(f"<p style='color:{MUTED}; font-size:0.85rem;'>Zonas: <b>Stomp Favor</b> (arriba), <b>Stomp Contra</b> (abajo), <b>Equilibrada</b> (centro).</p>", unsafe_allow_html=True)
        
        # Base scatter
        fig_gold = px.scatter(
            df_anom, x="duration_m", y="gold_diff", color="is_outlier",
            symbol="win", color_discrete_map={"Normal": "#60A5FA", "Outlier": "#F87171"},
            hover_name="match_id",
            labels={"duration_m": "Duración (min)", "gold_diff": "Dif. Oro", "is_outlier": "Tipo"},
            custom_data=["outlier_type", "total_kills", "win"]
        )
        
        # Add visual zones (Rectangles)
        fig_gold.add_hrect(y0=8000, y1=max(df_anom['gold_diff'].max(), 10000), fillcolor="green", opacity=0.05, line_width=0, annotation_text="Stomp Favor", annotation_position="top left")
        fig_gold.add_hrect(y0=-8000, y1=min(df_anom['gold_diff'].min(), -10000), fillcolor="red", opacity=0.05, line_width=0, annotation_text="Stomp Contra", annotation_position="bottom left")
        fig_gold.add_hrect(y0=-2000, y1=2000, fillcolor="gray", opacity=0.1, line_width=0, annotation_text="Equilibrada", annotation_position="top right")
        
        # Phase lines
        for x_val in [20, 30, 40]:
            fig_gold.add_vline(x=x_val, line_dash="dash", line_color=BORDER, line_width=1)
        fig_gold.add_hline(y=0, line_color=MUTED, line_width=1)
        
        # Highlighting outliers
        fig_gold.update_traces(
            marker=dict(size=12),
            selector=dict(name="Outlier")
        )
        fig_gold.update_traces(
            marker=dict(size=8, opacity=0.4),
            selector=dict(name="Normal")
        )
        
        # Annotations for top 5 outliers
        top_outliers = df_anom[df_anom['is_outlier'] == "Outlier"].sort_values("decision_score").head(5)
        for _, row in top_outliers.iterrows():
            fig_gold.add_annotation(x=row['duration_m'], y=row['gold_diff'], text=row['outlier_type'].split(",")[0], showarrow=True, arrowhead=1, arrowcolor=GOLD, font=dict(color=GOLD, size=9))

        fig_gold.update_traces(hovertemplate="<b>Match: %{hovertext}</b><br>Resultado: %{customdata[2]}<br>Duración: %{x:.1f}m<br>Dif. Oro: %{y}<br>Kills: %{customdata[1]}<br>Tipo: %{customdata[0]}<extra></extra>")
        fig_gold.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor=PAPER, font=dict(color=TEXT), height=450, margin=dict(t=30), yaxis=dict(gridcolor=BORDER))
        st.plotly_chart(fig_gold, use_container_width=True)

    with col2:
        st.markdown("#### Kills Totales vs Duración")
        st.markdown(f"<p style='color:{MUTED}; font-size:0.85rem;'>Zonas: <b>Fiesta</b> (arriba), <b>Macro</b> (abajo). Línea: Tendencia normal.</p>", unsafe_allow_html=True)
        
        fig_kills = px.scatter(
            df_anom, x="total_kills", y="duration_m", color="is_outlier",
            color_discrete_map={"Normal": "#34D399", "Outlier": "#F87171"},
            hover_name="match_id",
            labels={"total_kills": "Kills Totales", "duration_m": "Duración (min)"},
            custom_data=["outlier_type", "gold_diff", "win"]
        )
        
        # Trend line (Kills approx 1.8 * duration_m - 10)
        x_trend = np.linspace(df_anom['total_kills'].min(), df_anom['total_kills'].max(), 100)
        y_trend = (x_trend + 10) / 1.8
        fig_kills.add_trace(go.Scatter(x=x_trend, y=y_trend, mode='lines', name='Tendencia Normal', line=dict(color=MUTED, dash='dot')))
        
        # Visual zones (Shaded)
        fig_kills.add_vrect(x0=60, x1=max(df_anom['total_kills'].max(), 80), fillcolor="orange", opacity=0.05, line_width=0, annotation_text="Fiesta", annotation_position="top right")
        fig_kills.add_hrect(y0=40, y1=max(df_anom['duration_m'].max(), 50), fillcolor="blue", opacity=0.05, line_width=0, annotation_text="Larga/Macro", annotation_position="bottom left")
        
        fig_kills.update_traces(
            marker=dict(size=12),
            selector=dict(name="Outlier")
        )
        fig_kills.update_traces(
            marker=dict(size=8, opacity=0.4),
            selector=dict(name="Normal")
        )
        
        fig_kills.update_traces(hovertemplate="<b>Match: %{hovertext}</b><br>Resultado: %{customdata[2]}<br>Kills: %{x}<br>Duración: %{y:.1f}m<br>Dif. Oro: %{customdata[1]}<br>Tipo: %{customdata[0]}<extra></extra>")
        fig_kills.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor=PAPER, font=dict(color=TEXT), height=450, margin=dict(t=30), yaxis=dict(gridcolor=BORDER))
        st.plotly_chart(fig_kills, use_container_width=True)

    # 2. Distributions & Boxplots
    st.markdown("#### Análisis de Distribución y Outliers")
    st.markdown(f"<p style='color:{MUTED}; font-size:0.85rem;'>Boxplots con jitter (puntos) para observar la dispersión y los valores extremos de cada métrica.</p>", unsafe_allow_html=True)
    
    b1, b2, b3, b4 = st.columns([1, 1, 1, 1])
    
    with b1:
        fig_dur = px.box(df_anom, x="is_outlier", y="duration_m", color="is_outlier", 
                         points="all", color_discrete_map={"Normal": "#A1A1AA", "Outlier": "#F87171"},
                         labels={"duration_m": "Duración (min)", "is_outlier": ""})
        fig_dur.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor=PAPER, font=dict(color=TEXT), showlegend=False, height=400, margin=dict(t=10, b=10), yaxis=dict(gridcolor=BORDER))
        st.plotly_chart(fig_dur, use_container_width=True)

    with b2:
        fig_gold_box = px.box(df_anom, x="is_outlier", y="gold_diff", color="is_outlier", 
                              points="all", color_discrete_map={"Normal": "#60A5FA", "Outlier": "#F87171"},
                              labels={"gold_diff": "Dif. Oro", "is_outlier": ""})
        fig_gold_box.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor=PAPER, font=dict(color=TEXT), showlegend=False, height=400, margin=dict(t=10, b=10), yaxis=dict(gridcolor=BORDER))
        st.plotly_chart(fig_gold_box, use_container_width=True)

    with b3:
        fig_dmg_box = px.box(df_anom, x="is_outlier", y="damage_diff", color="is_outlier", 
                             points="all", color_discrete_map={"Normal": "#34D399", "Outlier": "#F87171"},
                             labels={"damage_diff": "Dif. Daño", "is_outlier": ""})
        fig_dmg_box.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor=PAPER, font=dict(color=TEXT), showlegend=False, height=400, margin=dict(t=10, b=10), yaxis=dict(gridcolor=BORDER))
        st.plotly_chart(fig_dmg_box, use_container_width=True)

    with b4:
        fig_kill_box = px.box(df_anom, x="is_outlier", y="kill_diff", color="is_outlier", 
                              points="all", color_discrete_map={"Normal": "#FCD34D", "Outlier": "#F87171"},
                              labels={"kill_diff": "Dif. Kills", "is_outlier": ""})
        fig_kill_box.update_layout(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor=PAPER, font=dict(color=TEXT), showlegend=False, height=400, margin=dict(t=10, b=10), yaxis=dict(gridcolor=BORDER))
        st.plotly_chart(fig_kill_box, use_container_width=True)

    # 3. Tables & Radar
    st.markdown("---")
    st.markdown("#### Partidas más Anómalas")
    
    # Showcase top outliers
    outliers = df_anom[df_anom['is_outlier'] == "Outlier"].sort_values("decision_score")
    if not outliers.empty:
        st.dataframe(
            outliers[["match_id", "duration_m", "kill_diff", "gold_diff", "outlier_type", "decision_score"]].style.format({"duration_m": "{:.1f}", "decision_score": "{:.4f}"}),
            use_container_width=True, hide_index=True
        )
        
        # Radar Chart for comparison
        st.markdown("#### Análisis de Radar: Partida vs Media")
        selected_match = st.selectbox("Selecciona una partida anómala para comparar:", outliers['match_id'].tolist())
        
        if selected_match:
            match_data = df_anom[df_anom['match_id'] == selected_match].iloc[0]
            avg_data = df_anom.mean(numeric_only=True)
            
            radar_cols = ["team_total_objs", "team_kills", "avg_kda_team", "avg_vision_team", "avg_cs_team"]
            # Normalizar para radar
            def get_radar_val(col):
                val = match_data[col]
                avg = avg_data[col]
                return (val / avg if avg > 0 else 1) * 100 # Porcentaje sobre la media
            
            vals = [get_radar_val(c) for c in radar_cols]
            
            fig_radar = go.Figure()
            fig_radar.add_trace(go.Scatterpolar(r=vals, theta=radar_cols, fill='toself', name='Partida Seleccionada', line=dict(color=GOLD)))
            fig_radar.add_trace(go.Scatterpolar(r=[100]*len(radar_cols), theta=radar_cols, line=dict(color=MUTED, dash='dash'), name='Media Global'))
            
            fig_radar.update_layout(
                polar=dict(radialaxis=dict(visible=True, range=[0, max(max(vals), 150)], gridcolor="#27272A"), bgcolor=PAPER),
                paper_bgcolor="rgba(0,0,0,0)", font=dict(color=TEXT), showlegend=True, height=500
            )
            st.plotly_chart(fig_radar, use_container_width=True)
    else:
        st.info("No se han detectado anomalías con el nivel de sensibilidad actual.")


def render_apriori_tab(pool_id, queue_id, min_friends):
    st.markdown("<h3 style='color:"+GOLD+";'>Reglas de Asociación (Apriori)</h3>", unsafe_allow_html=True)
    st.markdown(
        f"<p style='color:{MUTED}; margin-bottom:25px;'>"
        "Descubre patrones y sinergias ocultas en las partidas usando el algoritmo Apriori. "
        "El análisis se realiza a nivel de equipo (aliado vs enemigo)."
        "</p>",
        unsafe_allow_html=True
    )

        # 1. Filtros de Minería
    with st.expander("Configuración del Algoritmo", expanded=True):
        c1, c2, c3 = st.columns(3)
        with c1:
            min_support = st.slider("Soporte Mínimo", 0.01, 0.20, 0.03, 0.01, help="Frecuencia mínima de la combinación.")
        with c2:
            min_confidence = st.slider("Confianza Mínima", 0.1, 1.0, 0.6, 0.05, help="Fiabilidad del patrón.")
        with c3:
            max_len = st.number_input("Max. Items en Antecedente", 1, 5, 3)

    with st.spinner("Ejecutando minería enriquecida..."):
        df_raw = get_apriori_raw_data(pool_id, queue_id, min_friends)
        
        if df_raw.empty:
            st.warning("No hay suficientes datos.")
            return

        # 2. Generar Transacciones (Enriquecidas)
        transactions = []
        ROLE_MAP_INTERN = {
            "TOP": "TOP", "JUNGLE": "JUNGLE", "MIDDLE": "MID", 
            "BOTTOM": "ADC", "UTILITY": "SUPP"
        }
        
        for (match_id, team_id), group in df_raw.groupby(['match_id', 'team_id']):
            t = []
            win = group['win'].iloc[0]
            t.append("RESULT_WIN" if win else "RESULT_LOSS")
            
            # Aliados
            for _, row in group.iterrows():
                chanp = row['champion_name']
                role = ROLE_MAP_INTERN.get(row['role'], "UNKNOWN")
                t.append(f"ALLY_CHAMP_{chanp}")
                if role != "UNKNOWN":
                    t.append(f"ALLY_{role}_{chanp}")
            
            # Enemigos
            enemy_group = df_raw[(df_raw['match_id'] == match_id) & (df_raw['team_id'] != team_id)]
            for _, row in enemy_group.iterrows():
                chanp = row['champion_name']
                role = ROLE_MAP_INTERN.get(row['role'], "UNKNOWN")
                t.append(f"ENEMY_CHAMP_{chanp}")
                if role != "UNKNOWN":
                    t.append(f"ENEMY_{role}_{chanp}")
            
            transactions.append(list(set(t))) # Evitar duplicados exactos

        # 3. Apriori Pipeline
        from mlxtend.frequent_patterns import apriori, association_rules
        from mlxtend.preprocessing import TransactionEncoder
        
        te = TransactionEncoder()
        te_ary = te.fit(transactions).transform(transactions)
        df_onehot = pd.DataFrame(te_ary, columns=te.columns_)
        
        frequent_itemsets = apriori(df_onehot, min_support=min_support, use_colnames=True)
        
        if frequent_itemsets.empty:
            st.info("No se encontraron itemsets frecuentes.")
            return
            
        rules = association_rules(frequent_itemsets, metric="confidence", min_threshold=min_confidence)
        
        if rules.empty:
            st.info("No se generaron reglas.")
            return

        # --- REFINAMIENTO SEGURO (CALIDAD ENRIQUECIDA) ---
        
        # 1. Filtro de redundancia interna (Antecedente no puede tener CHAMP_X y ROLE_X al mismo tiempo)
        def has_internal_redundancy(ant):
            champs = [a.split("_")[-1] for a in ant if "_CHAMP_" in a]
            for a in ant:
                if "_CHAMP_" not in a and (a.startswith("ALLY_") or a.startswith("ENEMY_")):
                    # Si es ALLY_JUNGLE_LeeSin, el champ es LeeSin
                    c_name = a.split("_")[-1]
                    if c_name in champs: return True
            return False

        rules['internal_redundant'] = rules['antecedents'].apply(has_internal_redundancy)
        rules = rules[rules['internal_redundant'] == False].copy()

        # 2. Consecuente debe ser un RESULTADO y Lift > 1.1
        rules = rules[
            (rules['consequents'].apply(lambda x: any(c in ['RESULT_WIN', 'RESULT_LOSS'] for c in list(x)))) &
            (rules['lift'] > 1.1)
        ].copy()
        
        rules['antecedent_len'] = rules['antecedents'].apply(lambda x: len(x))
        rules = rules[rules['antecedent_len'] <= max_len].copy()

        # 3. ELIMINACIÓN DE REDUNDANCIA ENTRE REGLAS
        def remove_redundant(df):
            if df.empty: return df
            df = df.sort_values(['antecedent_len', 'lift'], ascending=[True, False])
            keep_indices = []
            
            for idx, row in df.iterrows():
                ant = row['antecedents']
                cons = row['consequents']
                is_redundant = False
                for k_idx in keep_indices:
                    prev_row = df.loc[k_idx]
                    if prev_row['consequents'] == cons:
                        # Si el nuevo antecedente es un superconjunto del anterior
                        if prev_row['antecedents'].issubset(ant):
                            if row['lift'] < prev_row['lift'] * 1.1:
                                is_redundant = True; break
                        # NUEVO: Si el anterior es CHAMP_X y el nuevo es ROLE_X (o viceversa)
                        # Este caso es más complejo, pero la lógica de issubset ya cubre gran parte
                if not is_redundant: keep_indices.append(idx)
            return df.loc[keep_indices]

        filtered_rules = remove_redundant(rules)
        filtered_rules = filtered_rules.sort_values(["lift", "confidence"], ascending=[False, False])

        if filtered_rules.empty:
            st.warning("No se encontraron reglas útiles.")
            return

    # 4. Visualizaciones
    
    def clean_label(item):
        if item.startswith("ALLY_CHAMP_"): return f"Aliado: {item.replace('ALLY_CHAMP_', '')}"
        if item.startswith("ENEMY_CHAMP_"): return f"Rival: {item.replace('ENEMY_CHAMP_', '')}"
        if item.startswith("ALLY_") and not item.startswith("ALLY_CHAMP_"):
            parts = item.split("_")
            return f"Aliado {parts[1]}: {parts[2]}"
        if item.startswith("ENEMY_") and not item.startswith("ENEMY_CHAMP_"):
            parts = item.split("_")
            return f"Rival {parts[1]}: {parts[2]}"
        if item == "RESULT_WIN": return "VICTORIA"
        if item == "RESULT_LOSS": return "DERROTA"
        return item

    def clean_set(s):
        return " + ".join([clean_label(i) for i in s])

    st.markdown("#### Patrones Estratégicos (Top 20 por Lift)")
    st.markdown(f"<p style='color:{MUTED}; font-size:0.85rem;'>Grosor del patrón (Lift) vs Fiabilidad (Color: Confianza).</p>", unsafe_allow_html=True)
    
    top_rules = filtered_rules.head(20).copy()
    top_rules['rule_str'] = top_rules.apply(lambda r: f"{clean_set(r['antecedents'])}  ➔  {clean_set(r['consequents'])}", axis=1)
    
    fig_lift = px.bar(
        top_rules, x="lift", y="rule_str", orientation='h',
        color="confidence", color_continuous_scale="Viridis",
        labels={"lift": "Fuerza (Lift)", "rule_str": "Condición", "confidence": "Confianza"},
    )
    fig_lift.update_layout(
        plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor=PAPER, font=dict(color=TEXT), 
        height=int(max(400, len(top_rules) * 30)), yaxis=dict(autorange="reversed"),
        xaxis=dict(gridcolor=BORDER)
    )
    st.plotly_chart(fig_lift, use_container_width=True)

    # 5. Tabla de Interpretación
    st.markdown("#### Listado de Patrones Estratégicos")
    
    def interpret(row):
        ants = list(row['antecedents'])
        cons = list(row['consequents'])
        ant_desc = [clean_label(a) for a in ants]
        res = "la probabilidad de **VICTORIA** aumenta" if "RESULT_WIN" in cons else "la probabilidad de **DERROTA** aumenta"
        return f"Cuando {' y '.join(ant_desc)}, {res} respecto a lo esperado."

    display_df = filtered_rules.head(50).copy()
    display_df['Interpretación'] = display_df.apply(interpret, axis=1)
    display_df['Fiabilidad'] = (display_df['confidence'] * 100).round(1).astype(str) + "%"
    
    st.dataframe(
        display_df[['Interpretación', 'Fiabilidad', 'lift']],
        column_config={"lift": st.column_config.NumberColumn("Fuerza (Lift)", format="%.2f")},
        use_container_width=True, hide_index=True
    )


def render_synergy_net_tab(pool_id, queue_id, min_friends):
    st.markdown("<h3 style='color:"+GOLD+";'>Red de Sinergias de Campeones</h3>", unsafe_allow_html=True)
    st.markdown(
        f"<p style='color:{MUTED}; margin-bottom:25px;'>"
        "Analiza qué campeones funcionan mejor juntos comparando su winrate conjunto "
        "contra lo esperado basándose en sus rendimientos individuales."
        "</p>",
        unsafe_allow_html=True
    )

    # 1. Filtros
    c_f1, c_f2 = st.columns([1, 2])
    with c_f1:
        min_matches_pair = st.slider("Mínimo partidas juntas", 1, 50, 20, help="Filtra pares con pocas partidas para mayor fiabilidad estadística.")
    
    with st.spinner("Calculando red de sinergias..."):
        df_raw = get_apriori_raw_data(pool_id, queue_id, min_friends)
        if df_raw.empty:
            st.warning("No hay suficientes datos para el análisis de sinergias.")
            return

        # A. Winrates Individuales
        champ_stats = df_raw.groupby('champion_name').agg(
            matches=('match_id', 'nunique'),
            wins=('win', lambda x: x.sum() / 5), # Cada victoria se cuenta 5 veces en el df_raw (uno por player)
            role=('role', lambda x: x.mode()[0] if not x.mode().empty else "UNKNOWN")
        ).reset_index()
        # Corregir cálculo de victorias (en el df de apriori hay una fila por player)
        # Es mejor agrupar por match/team para los winrates individuales reales
        df_team_win = df_raw.groupby(['match_id', 'team_id']).agg({'win': 'first'}).reset_index()
        champ_wins = df_raw.merge(df_team_win, on=['match_id', 'team_id'], suffixes=('', '_real'))
        champ_stats = champ_wins.groupby('champion_name').agg(
            matches=('match_id', 'nunique'),
            wins=('win_real', 'sum'),
            role=('role', lambda x: x.mode()[0] if not x.mode().empty else "UNKNOWN")
        ).reset_index()
        champ_stats['winrate'] = champ_stats['wins'] / champ_stats['matches']

        # B. Generar Pares
        pair_data = []
        for (match_id, team_id), group in df_raw.groupby(['match_id', 'team_id']):
            champs = sorted(group['champion_name'].tolist())
            win = group['win'].iloc[0]
            for combo in itertools.combinations(champs, 2):
                pair_data.append({"c1": combo[0], "c2": combo[1], "win": 1 if win else 0})
        
        df_pairs = pd.DataFrame(pair_data)
        if df_pairs.empty:
            st.warning("No se pudieron generar pares de campeones.")
            return

        df_pair_stats = df_pairs.groupby(['c1', 'c2']).agg(
            matches=('win', 'count'),
            wins=('win', 'sum')
        ).reset_index()
        df_pair_stats['winrate_pair'] = df_pair_stats['wins'] / df_pair_stats['matches']

        # C. Synergy Score
        wr_dict = dict(zip(champ_stats['champion_name'], champ_stats['winrate']))
        def calc_synergy(row):
            wr1 = wr_dict.get(row['c1'], 0.5)
            wr2 = wr_dict.get(row['c2'], 0.5)
            return row['winrate_pair'] - (wr1 + wr2) / 2
        
        df_pair_stats['synergy_score'] = df_pair_stats.apply(calc_synergy, axis=1)
        
        # Filtrar
        df_filtered = df_pair_stats[df_pair_stats['matches'] >= min_matches_pair].copy()

    if df_filtered.empty:
        st.info(f"No hay pares con al menos {min_matches_pair} partidas juntas. Prueba bajando el filtro.")
        return

    # 2. Visualización: Grafo (Network)
    st.markdown("#### Champion Synergy Network")
    st.markdown(f"<p style='color:{MUTED}; font-size:0.85rem;'>Mostrando las 30 sinergias más fuertes. Grosor = Sinergia, Tamaño = Popularidad.</p>", unsafe_allow_html=True)
    
    top_30_syn = df_filtered.sort_values("synergy_score", ascending=False).head(30)
    
    G = nx.Graph()
    for _, row in top_30_syn.iterrows():
        if row['synergy_score'] > 0:
            G.add_edge(row['c1'], row['c2'], weight=row['synergy_score'], matches=row['matches'], wr=row['winrate_pair'])

    pos = nx.spring_layout(G, k=0.5, iterations=50)

    # Edge traces
    edge_x = []
    edge_y = []
    edge_widths = []
    for edge in G.edges(data=True):
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_x.extend([x0, x1, None])
        edge_y.extend([y0, y1, None])
        edge_widths.append(edge[2]['weight'] * 20) # Scaling factor

    # Como plotly Scatter no soporta anchos variables por segmento fácilmente, 
    # dibujamos las líneas una a una si queremos grosores reales, o usamos una media.
    # Para simplicidad y rendimiento, usaremos trazos individuales.
    fig_net = go.Figure()
    
    for edge in G.edges(data=True):
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        fig_net.add_trace(go.Scatter(
            x=[x0, x1], y=[y0, y1],
            line=dict(width=max(1, edge[2]['weight'] * 40), color='rgba(150, 150, 150, 0.4)'),
            hoverinfo='none', mode='lines'
        ))

    # Node traces
    node_x = []
    node_y = []
    node_text = []
    node_size = []
    node_color = []
    
    ROLE_COLORS = {
        "TOP": "#F87171", "JUNGLE": "#60A5FA", "MIDDLE": "#34D399", 
        "BOTTOM": "#FCD34D", "UTILITY": "#A78BFA", "UNKNOWN": "#A1A1AA"
    }

    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        stats = champ_stats[champ_stats['champion_name'] == node].iloc[0]
        node_text.append(f"<b>{node}</b><br>Partidas: {stats['matches']}<br>Winrate: {stats['winrate']:.1%}")
        node_size.append(max(20, np.sqrt(stats['matches']) * 5))
        node_color.append(ROLE_COLORS.get(stats['role'], "#A1A1AA"))

    fig_net.add_trace(go.Scatter(
        x=node_x, y=node_y, mode='markers+text',
        text=[n for n in G.nodes()], textposition="top center",
        hovertext=node_text, hoverinfo='text',
        marker=dict(size=node_size, color=node_color, line_width=2, line_color=BG)
    ))

    fig_net.update_layout(
        showlegend=False, hovermode='closest',
        margin=dict(b=20,l=5,r=5,t=40),
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        height=700
    )
    st.plotly_chart(fig_net, use_container_width=True)

    # 3. Tablas
    st.markdown("---")
    c1, c2 = st.columns(2)
    
    def get_interpretation(row):
        return f"**{row['c1']}** y **{row['c2']}** tienen un winrate del **{row['winrate_pair']:.1%}**, {abs(row['synergy_score']):.1%} {'superior' if row['synergy_score'] > 0 else 'inferior'} a lo esperado por sus rendimientos individuales."

    with c1:
        st.markdown("#### Top 20 Sinergias (Mejor de lo esperado)")
        top_df = df_filtered.sort_values("synergy_score", ascending=False).head(20).copy()
        top_df['Interpretación'] = top_df.apply(get_interpretation, axis=1)
        st.dataframe(
            top_df[['c1', 'c2', 'winrate_pair', 'synergy_score', 'matches', 'Interpretación']],
            column_config={
                "winrate_pair": st.column_config.NumberColumn("Winrate Conjunto", format="%.1%"),
                "synergy_score": st.column_config.NumberColumn("Synergy Score", format="+.2f"),
                "matches": "Partidas"
            },
            use_container_width=True, hide_index=True
        )

    with c2:
        st.markdown("#### Top 20 Anti-Sinergias (Peor de lo esperado)")
        bot_df = df_filtered.sort_values("synergy_score", ascending=True).head(20).copy()
        bot_df['Interpretación'] = bot_df.apply(get_interpretation, axis=1)
        st.dataframe(
            bot_df[['c1', 'c2', 'winrate_pair', 'synergy_score', 'matches', 'Interpretación']],
            column_config={
                "winrate_pair": st.column_config.NumberColumn("Winrate Conjunto", format="%.1%"),
                "synergy_score": st.column_config.NumberColumn("Synergy Score", format="+.2f"),
                "matches": "Partidas"
            },
            use_container_width=True, hide_index=True
        )

    # 4. Heatmap
    st.markdown("#### Mapa de Calor de Winrate Combinado")
    # Pivotar para el heatmap
    top_champs = champ_stats.sort_values("matches", ascending=False).head(30)['champion_name'].tolist()
    df_h_pivot = df_filtered[
        (df_filtered['c1'].isin(top_champs)) & (df_filtered['c2'].isin(top_champs))
    ].pivot(index="c1", columns="c2", values="winrate_pair")
    
    fig_heat = px.imshow(
        df_h_pivot, color_continuous_scale="RdYlGn",
        labels=dict(color="Winrate"), text_auto=".0%"
    )
    fig_heat.update_layout(paper_bgcolor=PAPER, plot_bgcolor="rgba(0,0,0,0)", height=800)
    st.plotly_chart(fig_heat, use_container_width=True)


def render(pool_id: str, queue_id: int, min_friends: int):
    st.markdown("<h2 class='lol-header2'>Minería de Datos</h2>", unsafe_allow_html=True)
    
    tab_list = ["Champions Fiesta", "Composiciones Enemigas Peligrosas", "Detección de Anomalías", "Reglas de Asociación", "Red de Sinergias"]
    tabs = st.tabs(tab_list)
    
    with tabs[0]:
        render_fiesta_tab(pool_id, queue_id, min_friends)
        
    with tabs[1]:
        render_dangerous_tab(pool_id, queue_id, min_friends)
        
    with tabs[2]:
        render_anomalies_tab(pool_id, queue_id, min_friends)
        
    with tabs[3]:
        render_apriori_tab(pool_id, queue_id, min_friends)
        
    with tabs[4]:
        render_synergy_net_tab(pool_id, queue_id, min_friends)
