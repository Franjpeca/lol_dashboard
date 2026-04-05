"""
dashboard/pages/analisis.py
Página de análisis con mapas de calor y métricas cruzadas.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dashboard.db import get_matches_heatmap, get_winrate_evolution_data
from dashboard.theme import BG, PAPER, TEXT, GOLD, BORDER, MUTED

def render(pool_id: str, queue_id: int, min_friends: int):
    st.markdown("<h2 class='lol-header2'>Análisis</h2>", unsafe_allow_html=True)
    
    tab_heatmap, tab_network, tab_identity, tab_landscape, tab_sankey, tab_evolution = st.tabs([
        "Mapa de Calor (Horarios)", 
        "Red de Jugadores", 
        "Posiciones y Campeones", 
        "Duración de partidas y kills", 
        "Objetivos",
        "Evolución de Winrate"
    ])
    
    with tab_heatmap:
        st.markdown("### Distribución horaria")
        st.markdown(
            f"<p style='color:{MUTED}; margin-bottom:20px;'>"
            "Muestra en qué franjas horarias se concentran las partidas (independiente de quién juegue). "
            "El 'día' lógico comienza a las 07:00 AM para agrupar correctamente las sesiones de madrugada con el día anterior."
            "</p>", 
            unsafe_allow_html=True
        )
        
        df_heat = get_matches_heatmap(pool_id, queue_id, min_friends)
        if df_heat.empty:
            st.warning("No hay datos suficientes para generar el mapa de calor.")
        else:
            hours_order = list(range(7, 24)) + list(range(0, 7))
            days_order = {
                1: "Lunes", 2: "Martes", 3: "Miércoles", 4: "Jueves",
                5: "Viernes", 6: "Sábado", 7: "Domingo"
            }

            idx = pd.MultiIndex.from_product([list(days_order.keys()), hours_order], names=['logical_dow', 'hour_of_day'])
            full_grid = pd.DataFrame(index=idx).reset_index()
            
            merged = pd.merge(full_grid, df_heat, on=['logical_dow', 'hour_of_day'], how='left').fillna(0)
            matrix = merged.pivot(index='logical_dow', columns='hour_of_day', values='matches_count')
            
            matrix = matrix[hours_order]
            matrix.index = [days_order[d] for d in matrix.index]
            matrix.columns = [f"{int(h):02d}:00" for h in matrix.columns]

            fig = px.imshow(
                matrix.values,
                labels=dict(x="Hora", y="", color="Nº Partidas"),
                x=matrix.columns,
                y=matrix.index,
                color_continuous_scale="Turbo",
                aspect="auto"
            )
            
            fig.update_xaxes(side="top", tickangle=-45, showgrid=False, color=TEXT)
            fig.update_yaxes(showgrid=False, color=TEXT)
            
            fig.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor=PAPER,
                margin=dict(l=20, r=20, t=60, b=20),
                font=dict(family="Inter, sans-serif", color=TEXT),
                coloraxis_showscale=False,
                height=500
            )
            
            fig.update_traces(
                text=matrix.values,
                texttemplate="%{text}",
                textfont=dict(size=13, family="Inter, sans-serif")
            )

            st.plotly_chart(fig, use_container_width=True)

    with tab_network:
        st.markdown("### Red de Jugadores")
        st.markdown(
            f"<p style='color:{MUTED}; margin-bottom:10px;'>"
            "Explora las relaciones entre los jugadores. Las <b>líneas</b> representan dúos (grosor = partidas juntos, color = winrate del dúo). "
            "Los <b>círculos</b> representan a cada jugador individual (tamaño = partidas jugadas, color = winrate)."
            "</p>", 
            unsafe_allow_html=True
        )
        
        col_filter, _ = st.columns([1, 2])
        with col_filter:
            min_matches_slider = st.slider(
                "Mínimo de partidas compartidas (para mostrar conexión)",
                min_value=1,
                max_value=50,
                value=5,
                step=1,
                help="Filtra jugadores circunstanciales estableciendo un suelo de partidas jugadas juntos en el mismo equipo."
            )
            
        with st.spinner("Construyendo topología de la red..."):
            from dashboard.db import get_network_nodes, get_network_edges
            import networkx as nx
            import math
            
            df_edges = get_network_edges(pool_id, queue_id, min_friends, min_matches=min_matches_slider)
            
            if df_edges.empty:
                st.warning("No hay suficientes datos para armar la red con estos filtros. Prueba a reducir el mínimo de partidas.")
            else:
                valid_node_names = list(set(df_edges['player1'].tolist() + df_edges['player2'].tolist()))
                
                df_nodes = get_network_nodes(pool_id, queue_id, min_friends)
                df_nodes = df_nodes[df_nodes['name'].isin(valid_node_names)]
                
                if df_nodes.empty:
                    st.warning("Error interno: Nodos sin datos detectados.")
                else:
                    G = nx.Graph()
                    
                    for _, row in df_nodes.iterrows():
                        G.add_node(
                            row['name'], 
                            matches=row['matches'], 
                            winrate=row['winrate']
                        )

                    for _, row in df_edges.iterrows():
                        # Protect: ensure nodes exist before adding structural edge
                        if row['player1'] in G.nodes and row['player2'] in G.nodes:
                            G.add_edge(
                                row['player1'], 
                                row['player2'], 
                                matches=row['shared_matches'], 
                                winrate=row['duo_winrate'],
                                weight=row['shared_matches']
                            )

                    # Eliminar nodos aislados temporalmente para evitar layout errors (e.g. key errors in pos dict)
                    G.remove_nodes_from(list(nx.isolates(G)))

                    if len(G.nodes) == 0:
                         st.warning("No hay conexiones resultantes. Reduce el filtro.")
                    else:
                        try:
                            pos = nx.spring_layout(G, k=0.7, iterations=100, seed=42, weight=None)
                            
                            def get_color_for_winrate(wr):
                                # Scale wr from [0.4, 0.6] to [0, 1] for color interpolation
                                # Red (bad) at 40% or less, Green (good) at 60% or more
                                wr_val = float(wr)
                                t = min(1, max(0, (wr_val - 0.4) / 0.2))
                                r = int(255 * (1 - t))
                                g = int(255 * t)
                                return f"rgba({r}, {g}, 50, 0.7)"

                            # Map each node to its incident edges (by index in edge_traces) and neighbors
                            node_names_list = list(G.nodes()) # Map nodes consistently
                            node_edges = {n: [] for n in G.nodes()}
                            node_neighbors = {n: set() for n in G.nodes()}
                            
                            edge_traces = []
                            edge_idx = 0
                            for edge in G.edges(data=True):
                                n1, n2 = edge[0], edge[1]
                                m = edge[2]['matches']
                                wr = edge[2]['winrate']
                                
                                width = max(0.5, math.sqrt(m) * 0.2)
                                color = get_color_for_winrate(wr)
                                hover = f"{n1} & {n2}<br>Compartidas: {m}<br>WR Dúo: {wr*100:.1f}%"
                                
                                # Store relationships for JS filtering
                                node_edges[n1].append(edge_idx)
                                node_edges[n2].append(edge_idx)
                                node_neighbors[n1].add(n2)
                                node_neighbors[n2].add(n1)
                                
                                x0, y0 = pos[n1]
                                x1, y1 = pos[n2]
                                
                                n1_idx = node_names_list.index(n1)
                                n2_idx = node_names_list.index(n2)
                                cdata = {
                                    "type": "edge", 
                                    "id": edge_idx, 
                                    "n1": n1_idx, 
                                    "n2": n2_idx, 
                                    "txt1": f"<b>{n1}</b><br><span style='color:#FFD700;'>{m}p. ({wr*100:.0f}%)</span>", 
                                    "txt2": f"<b>{n2}</b><br><span style='color:#FFD700;'>{m}p. ({wr*100:.0f}%)</span>"
                                }
                                
                                edge_trace = go.Scatter(
                                    x=[x0, x1, None],
                                    y=[y0, y1, None],
                                    line=dict(width=width, color=color),
                                    hoverinfo='text',
                                    text=[hover, hover, hover],
                                    mode='lines',
                                    showlegend=False,
                                    opacity=0.8,
                                    customdata=[cdata, cdata, cdata]
                                )
                                edge_traces.append(edge_trace)
                                edge_idx += 1

                            node_x, node_y, node_text, node_hover, node_size, node_color = [], [], [], [], [], []
                            
                            for node in node_names_list:
                                x, y = pos[node]
                                node_x.append(x)
                                node_y.append(y)
                                
                                m = G.nodes[node]['matches']
                                wr = float(G.nodes[node]['winrate'])
                                
                                # Make nodes smaller to reduce cluster saturation
                                node_size.append(max(15, math.sqrt(m) * 1.5))
                                node_color.append(wr)
                                
                                # Base Hover Text
                                txt = f"<b>{node}</b><br>Partidas Totales: {m}<br>WR: {wr*100:.1f}%"
                                
                                node_hover.append(txt)
                                node_text.append(node if m >= 10 else "")

                            # Custom data encodes the relationships for each node
                            custom_data = []
                            for i, node in enumerate(node_names_list):
                                relevant_edges = node_edges[node]
                                
                                neighbor_info = {}
                                for nbr in G.neighbors(node):
                                    edata = G.get_edge_data(node, nbr)
                                    nbr_idx = node_names_list.index(nbr)
                                    neighbor_info[str(nbr_idx)] = f"<b>{nbr}</b><br><span style='color:#FFD700;'>{edata['matches']}p. ({edata['winrate']*100:.0f}%)</span>"
                                
                                custom_data.append({
                                    "type": "node", 
                                    "id": i, 
                                    "edges": relevant_edges, 
                                    "neighbors": neighbor_info
                                })

                            node_trace = go.Scatter(
                                x=node_x, y=node_y,
                                mode='markers+text',
                                text=node_text,
                                textfont=dict(color=TEXT, size=11),
                                textposition="top center",
                                hoverinfo='text',
                                hovertext=node_hover,
                                customdata=custom_data,
                                marker=dict(
                                    showscale=True,
                                    colorscale='RdYlGn', 
                                    cmin=0.4, cmax=0.6,
                                    color=node_color,
                                    size=node_size,
                                    colorbar=dict(
                                        thickness=12,
                                        title=dict(text='WR', side='right'),
                                        xanchor='left',
                                        tickformat=".0%"
                                    ),
                                    line_width=1.5,
                                    line_color=BG,
                                    opacity=0.9
                                ),
                                showlegend=False
                            )

                            fig2 = go.Figure(data=edge_traces + [node_trace])
                            fig2.update_layout(
                                hovermode='closest',
                                margin=dict(b=0, l=0, r=0, t=10),
                                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                plot_bgcolor='rgba(0,0,0,0)',
                                paper_bgcolor=PAPER,
                                font=dict(family="Inter, sans-serif", color=TEXT)
                            )

                            # Extract Plotly JSON and render custom HTML with inject JS for hover interactions
                            import json
                            import streamlit.components.v1 as components
                            
                            # Using full width and height
                            plot_json = fig2.to_json()
                            num_edges = len(edge_traces)
                            original_node_texts_js = json.dumps(node_text)
                            node_names_js = json.dumps(node_names_list)
                            
                            html_str = f"""
                            <html>
                            <head>
                                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
                                <style>
                                    body {{ margin: 0; background-color: {PAPER}; }}
                                    #plot {{ width: 100vw; height: 100vh; }}
                                </style>
                            </head>
                            <body>
                                <div id="plot"></div>
                                <script>
                                    var graphData = {plot_json};
                                    var originalNodeTexts = {original_node_texts_js};
                                    var nodeNamesList = {node_names_js};
                                    var plotDiv = document.getElementById('plot');
                                    
                                    Plotly.newPlot(plotDiv, graphData.data, graphData.layout, {{responsive: true, displayModeBar: true}});
                                    
                                    plotDiv.on('plotly_hover', function(data) {{
                                        var p = data.points[0];
                                        if(!p.customdata) return;
                                        
                                        var update = {{'opacity': [], 'marker.opacity': []}};
                                        var newNodeTexts = [...originalNodeTexts];
                                        
                                        if (p.customdata.type === 'node') {{
                                            var nodeId = p.customdata.id;
                                            var neighborInfo = p.customdata.neighbors; // Dictionary stringIdx -> text
                                            var edgeIndices = p.customdata.edges;
                                            
                                            // Dim unrelated edges
                                            for(let i=0; i<{num_edges}; i++) {{
                                                update['opacity'].push(edgeIndices.includes(i) ? 1.0 : 0.05);
                                                update['marker.opacity'].push(null);
                                            }}
                                            
                                            // Manage nodes: dim unrelated, show stats for neighbors
                                            var nodeOpacities = [];
                                            for(let i=0; i<{len(node_names_list)}; i++) {{
                                                if (i === nodeId) {{
                                                    nodeOpacities.push(0.9);
                                                    newNodeTexts[i] = "<b>" + nodeNamesList[i] + "</b>";
                                                }} else if (neighborInfo[i.toString()] !== undefined) {{
                                                    nodeOpacities.push(0.9);
                                                    newNodeTexts[i] = neighborInfo[i.toString()];
                                                }} else {{
                                                    nodeOpacities.push(0.1);
                                                    newNodeTexts[i] = ""; // clear names for distant nodes
                                                }}
                                            }}
                                            
                                            update['opacity'].push(1.0);
                                            update['marker.opacity'].push(nodeOpacities);
                                            
                                        }} else if (p.customdata.type === 'edge') {{
                                            var edgeId = p.customdata.id;
                                            var n1 = p.customdata.n1;
                                            var n2 = p.customdata.n2;
                                            var txt1 = p.customdata.txt1;
                                            var txt2 = p.customdata.txt2;
                                            
                                            for(let i=0; i<{num_edges}; i++) {{
                                                update['opacity'].push(i === edgeId ? 1.0 : 0.05);
                                                update['marker.opacity'].push(null);
                                            }}
                                            
                                            var nodeOpacities = [];
                                            for(let i=0; i<{len(node_names_list)}; i++) {{
                                                if (i === n1) {{
                                                    nodeOpacities.push(0.9);
                                                    newNodeTexts[i] = txt1;
                                                }} else if (i === n2) {{
                                                    nodeOpacities.push(0.9);
                                                    newNodeTexts[i] = txt2;
                                                }} else {{
                                                    nodeOpacities.push(0.1);
                                                    newNodeTexts[i] = "";
                                                }}
                                            }}
                                            
                                            update['opacity'].push(1.0);
                                            update['marker.opacity'].push(nodeOpacities);
                                        }} else {{
                                            return;
                                        }}
                                        
                                        var traceIndices = Array.from({{length: {num_edges} + 1}}, (v, i) => i);
                                        Plotly.restyle(plotDiv, update, traceIndices);
                                        Plotly.restyle(plotDiv, {{'text': [newNodeTexts]}}, [{num_edges}]);
                                    }});
                                    
                                    plotDiv.on('plotly_unhover', function(data) {{
                                        var update = {{'opacity': [], 'marker.opacity': []}};
                                        for(let i=0; i<{num_edges}; i++) {{
                                            update['opacity'].push(0.8);
                                            update['marker.opacity'].push(null);
                                        }}
                                        update['opacity'].push(1.0);
                                        update['marker.opacity'].push(Array({len(node_names_list)}).fill(0.9));
                                        
                                        var traceIndices = Array.from({{length: {num_edges} + 1}}, (v, i) => i);
                                        Plotly.restyle(plotDiv, update, traceIndices);
                                        Plotly.restyle(plotDiv, {{'text': [originalNodeTexts]}}, [{num_edges}]);
                                    }});
                                </script>
                            </body>
                            </html>
                            """
                            components.html(html_str, height=650)
                            
                        except Exception as e:
                            st.error(f"Error renderizando la red (JS Injection): {e}")

    with tab_identity:
        st.markdown("### Posiciones y Campeones")
        st.markdown(
            f"<p style='color:{MUTED}; margin-bottom:20px;'>"
            "Representa el estilo de cada jugador basado en el volumen de sus partidas. "
            "Muestra qué roles juega y con qué campeones dentro de cada rol. Cuanto más grande sea la sección, más partidas acumula."
            "</p>", 
            unsafe_allow_html=True
        )
        
        from dashboard.db import get_player_identity_distribution
        df_identity = get_player_identity_distribution(pool_id, queue_id, min_friends)
        
        if df_identity.empty:
            st.warning("No hay datos suficientes para perfilar las identidades en este filtro.")
        else:
            # Root node to glue everyone together
            df_identity['root'] = 'Comunidad'
            
            # Translate roles for better UI presentation
            role_map = {
                'TOP': 'Top',
                'JUNGLE': 'Jungla',
                'MIDDLE': 'Mid',
                'BOTTOM': 'ADC',
                'UTILITY': 'Support',
                # Legacy / Alternative mappings
                'CARRY': 'ADC',
                'SUPPORT': 'Support',
                'DUO_CARRY': 'ADC',
                'DUO_SUPPORT': 'Support',
                'DUO': 'Support' # A veces duo sin especificar asume rol bot, lo mapearemos a Support por si acaso
            }
            df_identity['role'] = df_identity['role'].map(lambda x: role_map.get(x, x))
            
            # Group by persona, role, champion to avoid duplicates after role mapping
            df_identity = df_identity.groupby(['persona', 'role', 'champion'], as_index=False).agg({
                'matches': 'sum',
                'wins': 'sum'
            })
            
            # Selector de jugador
            personas_list = sorted(df_identity['persona'].unique().tolist())
            selected_persona = st.selectbox(
                "Selecciona un jugador para ver su identidad en detalle:",
                ["Toda la Comunidad"] + personas_list
            )
            
            if selected_persona != "Toda la Comunidad":
                df_persona = df_identity[df_identity['persona'] == selected_persona].copy()
                
                ids, labels, parents, values, colors, hovers = [], [], [], [], [], []
                
                # Persona Colors
                player_color = '#31123B' 
                role_colors = {'Top': '#EF4444', 'Jungla': '#22C55E', 'Mid': '#3B82F6', 'ADC': '#F59E0B', 'Support': '#8B5CF6'}
                champ_colors = {'Top': '#B91C1C', 'Jungla': '#15803D', 'Mid': '#1D4ED8', 'ADC': '#B45309', 'Support': '#6D28D9'}
                
                # Center
                total_m = df_persona['matches'].sum()
                total_w = df_persona['wins'].sum()
                wr = (total_w / total_m * 100) if total_m > 0 else 0
                
                ids.append(selected_persona)
                labels.append(selected_persona)
                parents.append("")
                values.append(total_m)
                colors.append(player_color)
                hovers.append(f"<b>{selected_persona}</b><br>Partidas: {total_m}<br>Winrate: {wr:.1f}%")
                
                # Roles
                for role in df_persona['role'].unique():
                    df_r = df_persona[df_persona['role'] == role]
                    role_m = df_r['matches'].sum()
                    role_w = df_r['wins'].sum()
                    role_wr = (role_w / role_m * 100) if role_m > 0 else 0
                    
                    role_id = f"{selected_persona}/{role}"
                    ids.append(role_id)
                    labels.append(role)
                    parents.append(selected_persona)
                    values.append(role_m)
                    colors.append(role_colors.get(role, "#4B5563"))
                    p_parent = (role_m / total_m * 100) if total_m > 0 else 0
                    hovers.append(f"<b>{role}</b><br>Partidas: {role_m}<br>Winrate: {role_wr:.1f}%<br>Representa el {p_parent:.1f}% de {selected_persona}")
                    
                    # Champions
                    for _, row in df_r.iterrows():
                        c_m = row['matches']
                        c_w = row['wins']
                        c_wr = (c_w / c_m * 100) if c_m > 0 else 0
                        
                        # Champion name fallback
                        c_name = str(row['champion']) if row['champion'] else "Desconocido"
                        
                        ids.append(f"{role_id}/{c_name}")
                        labels.append(c_name)
                        parents.append(role_id)
                        values.append(c_m)
                        colors.append(champ_colors.get(role, "#9CA3AF"))
                        p_parent = (c_m / role_m * 100) if role_m > 0 else 0
                        hovers.append(f"<b>{c_name}</b><br>Partidas: {c_m}<br>Winrate: {c_wr:.1f}%<br>Representa el {p_parent:.1f}% en {role}")

                fig_sun = go.Figure(go.Sunburst(
                    ids=ids,
                    labels=labels,
                    parents=parents,
                    values=values,
                    marker=dict(colors=colors),
                    branchvalues='total',
                    customdata=hovers,
                    hovertemplate="%{customdata}<extra></extra>"
                ))
            else:
                # Dashboard general (Comunidad)
                df_plot = df_identity.copy()
                
                ids, labels, parents, values, hovers, colors = [], [], [], [], [], []
                
                root_m = df_plot['matches'].sum()
                root_w = df_plot['wins'].sum()
                root_wr = (root_w / root_m * 100) if root_m > 0 else 0
                
                ids.append("Comunidad")
                labels.append("Comunidad")
                parents.append("")
                values.append(root_m)
                hovers.append(f"<b>Comunidad</b><br>Partidas: {root_m}<br>Winrate: {root_wr:.1f}%")
                colors.append("#1F2937") # Gris oscuro para el root
                
                personas = sorted(df_plot['persona'].unique().tolist())
                turbo_colors = px.colors.sample_colorscale("Turbo", [i/(len(personas)-1 if len(personas)>1 else 1) for i in range(len(personas))])
                persona_to_color = dict(zip(personas, turbo_colors))
                
                # Personas
                for persona in personas:
                    df_p = df_plot[df_plot['persona'] == persona]
                    p_m = df_p['matches'].sum()
                    p_w = df_p['wins'].sum()
                    p_wr = (p_w / p_m * 100) if p_m > 0 else 0
                    
                    ids.append(persona)
                    labels.append(persona)
                    parents.append("Comunidad")
                    values.append(p_m)
                    hovers.append(f"<b>{persona}</b><br>Partidas: {p_m}<br>Winrate: {p_wr:.1f}%")
                    colors.append(persona_to_color[persona])
                    
                    # Roles de esa persona
                    for role in df_p['role'].unique():
                        df_pr = df_p[df_p['role'] == role]
                        pr_m = df_pr['matches'].sum()
                        pr_w = df_pr['wins'].sum()
                        pr_wr = (pr_w / pr_m * 100) if pr_m > 0 else 0
                        
                        pr_id = f"{persona}/{role}"
                        ids.append(pr_id)
                        labels.append(role)
                        parents.append(persona)
                        values.append(pr_m)
                        p_parent = (pr_m / p_m * 100) if p_m > 0 else 0
                        hovers.append(f"<b>{persona} - {role}</b><br>Partidas: {pr_m}<br>Winrate: {pr_wr:.1f}%<br>Representa el {p_parent:.1f}%")
                        colors.append(persona_to_color[persona])
                        
                        # Campeones
                        for _, row in df_pr.iterrows():
                            # Skip empty or unknown champions to avoid gaps
                            c_name = row['champion'] if row['champion'] else "Desconocido"
                            c_m = row['matches']
                            c_w = row['wins']
                            c_wr = (c_w / c_m * 100) if c_m > 0 else 0
                            
                            ids.append(f"{pr_id}/{c_name}")
                            labels.append(c_name)
                            parents.append(pr_id)
                            values.append(c_m)
                            p_parent = (c_m / pr_m * 100) if pr_m > 0 else 0
                            hovers.append(f"<b>{c_name} ({persona})</b><br>Partidas: {c_m}<br>Winrate: {c_wr:.1f}%<br>Representa el {p_parent:.1f}%")
                            colors.append(persona_to_color[persona])

                fig_sun = go.Figure(go.Sunburst(
                    ids=ids,
                    labels=labels,
                    parents=parents,
                    values=values,
                    branchvalues='total',
                    marker=dict(colors=colors),
                    customdata=hovers,
                    hovertemplate="%{customdata}<extra></extra>"
                ))
            
            fig_sun.update_layout(
                plot_bgcolor="#0B0F19",
                paper_bgcolor="#0B0F19",
                margin=dict(t=10, l=10, r=10, b=10),
                font=dict(family="Inter, sans-serif", color="#FFFFFF"),
                dragmode='pan',
                height=700
            )
            
            # Ensure labels are visible
            fig_sun.update_traces(textinfo="label")
            
            st.plotly_chart(fig_sun, use_container_width=True, config={'scrollZoom': True})

    with tab_landscape:
        st.markdown("### Duracion de partidas y kills")
        st.markdown(
            f"<p style='color:{MUTED}; margin-bottom:20px;'>"
            "Visualiza el ritmo e intensidad de las partidas. El eje X muestra la duración y el eje Y la agresividad (kills por minuto). "
            "Las bandas de fondo ayudan a identificar el tipo de partida: Stomp (rápida), Fast, Standard o Late game."
            "</p>", 
            unsafe_allow_html=True
        )
        
        from dashboard.db import get_match_landscape_data
        import numpy as np
        
        df_land = get_match_landscape_data(pool_id, queue_id, min_friends)
        if df_land.empty:
            st.warning("No hay datos suficientes para el Duracion de partidas y kills.")
        else:
            # Pre-procesamiento
            df_land['duration_m'] = df_land['duration_s'] / 60
            df_land['kpm'] = df_land['total_kills'] / df_land['duration_m']
            
            # Tamaño de punto: sqrt(total_kills) limitado
            df_land['point_size'] = np.sqrt(df_land['total_kills'])
            max_p_size = 20
            df_land['point_size'] = df_land['point_size'].clip(upper=max_p_size)
            
            fig_land = px.scatter(
                df_land,
                x='duration_m',
                y='kpm',
                size='point_size',
                size_max=max_p_size,
                opacity=0.25,
                hover_name='match_id',
                labels={
                    'duration_m': 'Duración (min)',
                    'kpm': 'Kills por Minuto',
                    'total_kills': 'Kills Totales'
                },
                custom_data=['match_id', 'duration_m', 'total_kills', 'kpm'],
                color_discrete_sequence=['#3B82F6'] # Un solo color elegante
            )
            
            # Bandas de fondo
            bands = [
                (0, 20, "Stomp", "rgba(239, 68, 68, 0.05)"),    # Rojo suave
                (20, 30, "Fast", "rgba(245, 158, 11, 0.05)"),   # Ámbar suave
                (30, 40, "Standard", "rgba(34, 197, 94, 0.05)"),# Verde suave
                (40, 100, "Late", "rgba(139, 92, 246, 0.05)"),  # Violeta suave
            ]
            
            for start, end, label, color in bands:
                fig_land.add_vrect(
                    x0=start, x1=end,
                    fillcolor=color,
                    layer="below",
                    line_width=0,
                    annotation_text=label,
                    annotation_position="top left",
                    annotation_font_size=12,
                    annotation_font_color=MUTED
                )
            
            fig_land.update_traces(
                hovertemplate="""
                <b>ID: %{customdata[0]}</b><br>
                Duración: %{customdata[1]:.1f} min<br>
                Kills Totales: %{customdata[2]}<br>
                Kills por Minuto: %{customdata[3]:.2f}
                <extra></extra>
                """
            )
            
            fig_land.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor=PAPER,
                margin=dict(l=20, r=20, t=20, b=20),
                font=dict(family="Inter, sans-serif", color=TEXT),
                xaxis=dict(showgrid=False, color=TEXT, range=[0, max(60, df_land['duration_m'].max() + 5)]),
                yaxis=dict(showgrid=True, gridcolor=BORDER, color=TEXT),
                height=600,
                showlegend=False
            )
            
            # Display logic with selection capture
            
            # Use a unique key for the plot to capture events
            event = st.plotly_chart(fig_land, use_container_width=True, on_select="rerun", selection_mode="points")
            
            # Capture selected point from custom_data
            if event and "selection" in event and event["selection"]["points"]:
                point = event["selection"]["points"][0]
                # custom_data index 0 is match_id
                selected_id = point["customdata"][0]
                st.session_state['selected_match_id'] = selected_id
            
            # Show Match ID below for easy copying
            selected_mid = st.session_state.get('selected_match_id', '')
            if selected_mid:
                st.markdown(f"**ID de Partida seleccionado:**")
                st.code(selected_mid)
                if st.button("Limpiar selección"):
                    del st.session_state['selected_match_id']
                    st.rerun()
            else:
                st.info("Haz clic en un punto para ver y copiar su Match ID.")
        
    with tab_sankey:
        st.markdown("### Análisis de Sinergias (Parejas)")
        st.markdown(
            f"<p style='color:{MUTED}; margin-bottom:20px;'>"
            "Compara la frecuencia y efectividad de obtener <b>parejas de objetivos</b> específicos. "
            "Ayuda a identificar qué combinaciones de dos objetivos son más decisivas."
            "</p>", 
            unsafe_allow_html=True
        )
        
        from dashboard.db import get_sankey_flow_data
        import itertools
        import numpy as np
        
        df_objs = get_sankey_flow_data(pool_id, queue_id, min_friends)
        
        if df_objs.empty:
            st.warning("No hay datos suficientes para el análisis de sinergias.")
        else:
            objs_cols = ['first_blood', 'tower', 'dragon', 'herald', 'grubs']
            obj_labels = {
                'first_blood': 'FB',
                'tower': 'Torre',
                'dragon': 'Dragón',
                'herald': 'Heraldo',
                'grubs': 'Grubs'
            }
            
            pairs_data = []
            for col1, col2 in itertools.combinations(objs_cols, 2):
                mask = (df_objs[col1] == True) & (df_objs[col2] == True)
                sub_df = df_objs[mask]
                matches_count = len(sub_df)
                
                if matches_count > 0:
                    winrate = (sub_df['result'] == 'Victoria').mean() * 100
                    pairs_data.append({
                        'Combinación': f"{obj_labels[col1]} + {obj_labels[col2]}",
                        'Partidas': matches_count,
                        'Winrate': round(winrate, 1),
                        'Lift': round(winrate - 50, 1),
                        'Lossrate': round(100 - winrate, 1)
                    })
            
            if not pairs_data:
                st.info("No hay suficientes partidas con múltiples objetivos para este análisis.")
            else:
                df_pairs = pd.DataFrame(pairs_data)
                avg_matches = df_pairs['Partidas'].mean()
                
                # Resumen superior
                top_freq = df_pairs.sort_values('Partidas', ascending=False).iloc[0]
                top_wr = df_pairs.sort_values('Winrate', ascending=False).iloc[0]
                # Best balance (frecuencia * winrate_lift)
                df_pairs['score'] = df_pairs['Partidas'] * (df_pairs['Winrate'] - 40).clip(0)
                best_bal = df_pairs.sort_values('score', ascending=False).iloc[0]
                
                m1, m2 = st.columns(2)
                m1.metric("Más Frecuente", top_freq['Combinación'], f"{top_freq['Partidas']} partidas", delta_color="off")
                m2.metric("Mejor Winrate", top_wr['Combinación'], f"{top_wr['Winrate']}% ({top_wr['Partidas']} partidas)", delta_color="off")
                
                # Lógica de etiquetas selectivas
                # Top 3 frecuencia, Top 3 winrate, Top 3 score
                label_candidates = set(df_pairs.sort_values('Partidas', ascending=False).head(3)['Combinación']) | \
                                  set(df_pairs.sort_values('Winrate', ascending=False).head(3)['Combinación']) | \
                                  set(df_pairs.sort_values('score', ascending=False).head(3)['Combinación'])
                
                df_pairs['Label'] = df_pairs['Combinación'].apply(lambda x: x if x in label_candidates else "")
                df_pairs['size_scaled'] = np.sqrt(df_pairs['Partidas']) * 5
                
                fig_pairs = px.scatter(
                    df_pairs,
                    x="Partidas",
                    y="Winrate",
                    size="size_scaled",
                    color="Winrate",
                    text="Label",
                    color_continuous_scale="RdYlGn",
                    height=550,
                    template="plotly_dark"
                )
                
                fig_pairs.update_traces(
                    textposition="top center",
                    marker=dict(line=dict(width=1, color='white')),
                    hovertemplate="""
                    <b>%{customdata[1]}</b><br>
                    Partidas: %{x}<br>
                    Winrate: <b>%{y}%</b>
                    <extra></extra>
                    """,
                    customdata=np.stack((df_pairs['Lossrate'], df_pairs['Combinación']), axis=-1)
                )
                
                fig_pairs.update_layout(
                    paper_bgcolor=PAPER,
                    plot_bgcolor="rgba(0,0,0,0)",
                    font=dict(family="Inter, sans-serif", color=TEXT),
                    margin=dict(l=40, r=40, t=20, b=40),
                    coloraxis_showscale=True,
                    coloraxis_colorbar=dict(title="Winrate", ticksuffix="%"),
                    xaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.05)"),
                    yaxis=dict(showgrid=True, gridcolor="rgba(255,255,255,0.05)")
                )
                
                # CSS para efecto enfoque (dim others on hover)
                st.markdown("""
                    <style>
                    /* Dim everything when hovering the chart container */
                    .hover-focus-chart .js-plotly-plot .plotly:hover .scatterlayer .trace .point,
                    .hover-focus-chart .js-plotly-plot .plotly:hover .scatterlayer .trace .textpoint {
                        opacity: 0.2 !important;
                        transition: opacity 0.2s ease;
                    }
                    /* Restore hovered element */
                    .hover-focus-chart .js-plotly-plot .plotly .scatterlayer .trace .point:hover,
                    .hover-focus-chart .js-plotly-plot .plotly .scatterlayer .trace .textpoint:hover {
                        opacity: 1 !important;
                    }
                    </style>
                """, unsafe_allow_html=True)

                st.markdown('<div class="hover-focus-chart">', unsafe_allow_html=True)
                st.plotly_chart(fig_pairs, use_container_width=True, theme=None)
                st.markdown('</div>', unsafe_allow_html=True)

        st.markdown("### Análisis de Sinergias (Parejas)")
        st.markdown(
            f"<p style='color:{MUTED}; margin-bottom:20px;'>"
            "Analiza cómo la primera ventaja obtenida influye en el tipo de partida y el resultado final. "
            "El flujo conecta la <b>Primera Ventaja</b> -> el <b>Ritmo de Partida</b> -> el <b>Resultado</b>."
            "</p>", 
            unsafe_allow_html=True
        )
        
        from dashboard.db import get_sankey_flow_data
        
        df_sankey_raw = get_sankey_flow_data(pool_id, queue_id, min_friends)
        
        if df_sankey_raw.empty:
            st.warning("No hay datos suficientes para generar los diagramas de flujo.")
        else:
            objectives_to_show = [
                ("Primera Torre", "tower"),
                ("Primer Dragón", "dragon"),
                ("Primer Heraldo", "herald"),
                ("First Grubs", "grubs")
            ]
            
            for obj_label, col_target in objectives_to_show:
                st.markdown(f"#### {obj_label}")
                
                # Preparar datos binarios: "SÍ [Objetivo]" vs "NO [Objetivo]"
                df_binary = df_sankey_raw.copy()
                yes_label = f"SÍ {obj_label}"
                no_label = f"NO {obj_label}"
                
                df_binary['source_node'] = df_binary[col_target].map({True: yes_label, False: no_label})
                
                # Nodos fijos
                labels = [yes_label, no_label, "Victoria", "Derrota"]
                label_to_idx = {label: i for i, label in enumerate(labels)}
                
                # Totales para porcentajes
                source_counts = df_binary['source_node'].value_counts().to_dict()
                result_counts = df_binary['result'].value_counts().to_dict()
                
                # Construir enlaces (Exactly 4 flows)
                links = []
                grouped = df_binary.groupby(['source_node', 'result']).size().reset_index(name='count')
                
                for _, row in grouped.iterrows():
                    src, res, val = row['source_node'], row['result'], int(row['count'])
                    p_src = (val / source_counts[src] * 100) if source_counts.get(src, 0) > 0 else 0
                    p_res = (val / result_counts[res] * 100) if result_counts.get(res, 0) > 0 else 0
                    
                    links.append({
                        'source': label_to_idx[src],
                        'target': label_to_idx[res],
                        'value': val,
                        'src_name': src,
                        'res_name': res,
                        'p_src': p_src,
                        'p_res': p_res
                    })

                fig_sankey = go.Figure(data=[go.Sankey(
                    arrangement = "snap",
                    node = dict(
                      pad = 30,
                      thickness = 20,
                      line = dict(color = "black", width = 0.5),
                      label = labels,
                      color = ["#4ADE80", "#F87171", "#3B82F6", "#94A3B8"], # Colores para Yes, No, Win, Loss
                      hoverlabel = dict(align="left", bgcolor="rgba(15, 23, 42, 0.9)"),
                      hovertemplate="""
                      <b>%{label}</b><br>
                      Partidas: %{value}<br>
                      <extra></extra>
                      """
                    ),
                    link = dict(
                      source = [l['source'] for l in links],
                      target = [l['target'] for l in links],
                      value = [l['value'] for l in links],
                      customdata = [ (
                          f"{l['src_name']} → {l['res_name']}", 
                          l['p_src'], l['src_name'], 
                          l['p_res'], l['res_name']
                      ) for l in links ],
                      color = "rgba(59, 130, 246, 0.2)",
                      hoverlabel = dict(align="left", bgcolor="rgba(15, 23, 42, 0.9)"),
                      hovertemplate="""
                      <b>%{customdata[0]}</b><br>
                      Partidas: %{value}<br><br>
                      • El <b>%{customdata[1]:.1f}%</b> de las partidas <b>%{customdata[2]}</b> acabaron en este resultado.<br>
                      • El <b>%{customdata[3]:.1f}%</b> de las partidas con resultado <b>%{customdata[4]}</b> tuvieron este inicio.<br>
                      <extra></extra>
                      """
                    )
                )])
                
                fig_sankey.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor=PAPER,
                    font=dict(family="Inter, sans-serif", color=TEXT, size=14),
                    height=350, # Algo más bajo para que quepan bien los 3
                    margin=dict(l=40, r=40, t=20, b=20)
                )
                
                st.plotly_chart(fig_sankey, use_container_width=True)
                st.write("---")
            
            # --- AGREGADO: UpSet Plot dentro de la misma pestaña ---
            st.markdown("### Frecuencia de objetivos y combinaciones")
            
            # Selector de orden y filtro
            col_sort, col_filter = st.columns([1, 2])
            with col_sort:
                sort_by = st.selectbox(
                    "Ordenar por:",
                    ["Número de Partidas", "Winrate"],
                    index=0,
                    key="upset_sort"
                )
            
            with col_filter:
                # Obtenemos los nombres legibles
                obj_labels = ["FB", "Torre", "Dragón", "Heraldo", "Grubs"]
                filter_objs = st.multiselect(
                    "Filtrar por objetivos (que deben estar presentes):",
                    obj_labels,
                    key="upset_filter_multi"
                )
            
            st.markdown(
                f"<p style='color:{MUTED}; margin-bottom:20px;'>"
                "Este gráfico analiza vuestra efectividad según la combinación exacta de objetivos conseguidos. "
                "Permite identificar si el valor de un objetivo (ej. Dragón) cambia cuando se combina con otros (ej. FB)."
                "</p>", 
                unsafe_allow_html=True
            )
            
            df_upset = df_sankey_raw.copy() # Reutilizamos los datos ya cargados
            
            # Procesar combinaciones
            objs_cols = ['first_blood', 'tower', 'dragon', 'herald', 'grubs']
            obj_names = {
                'first_blood': 'FB',
                'tower': 'Torre',
                'dragon': 'Dragón',
                'herald': 'Heraldo',
                'grubs': 'Grubs'
            }
            
            # Aplicar filtro si hay algo seleccionado
            if filter_objs:
                # Mapear etiquetas a columnas
                label_to_col = {v: k for k, v in obj_names.items()}
                for label in filter_objs:
                    col = label_to_col[label]
                    df_upset = df_upset[df_upset[col] == True]
            
            if df_upset.empty:
                st.info("No hay partidas con esa combinación exacta de objetivos.")
                return # O return de la función general
                
            def get_comb(row):
                active = [obj_names[c] for c in objs_cols if row[c]]
                return " + ".join(active) if active else "Ninguno"
            
            df_upset['comb'] = df_upset.apply(get_comb, axis=1)
            
            # Agrupar por combinación
            comb_stats = df_upset.groupby('comb').agg(
                count=('result', 'count'),
                wins=('result', lambda x: (x == 'Victoria').sum())
            ).reset_index()
            
            comb_stats['winrate'] = (comb_stats['wins'] / comb_stats['count'] * 100).round(1)
            total_matches_upset = comb_stats['count'].sum()
            comb_stats['pct_total'] = (comb_stats['count'] / total_matches_upset * 100).round(1)
            
            if sort_by == "Número de Partidas":
                comb_stats = comb_stats.sort_values('count', ascending=False)
            else:
                comb_stats = comb_stats.sort_values(['winrate', 'count'], ascending=[False, False])
            
            # --- Visualización UpSet ---
            from plotly.subplots import make_subplots
            
            fig = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.03,
                row_heights=[0.65, 0.35]
            )
            
            # 1. Bar Chart (Top)
            colors = []
            for wr in comb_stats['winrate']:
                if wr >= 60: colors.append('#4ADE80') # Green
                elif wr >= 45: colors.append('#FACC15') # Yellow
                else: colors.append('#F87171') # Red
                
            fig.add_trace(
                go.Bar(
                    x=comb_stats['comb'],
                    y=comb_stats['count'],
                    marker_color=colors,
                    text=comb_stats.apply(lambda r: f"{r['pct_total']}%<br>{r['winrate']}% wr", axis=1),
                    textposition='auto',
                    customdata=list(zip(comb_stats['winrate'], 100 - comb_stats['winrate'], comb_stats['pct_total'])),
                    hovertemplate="""
                    <b>%{x}</b><br>
                    Partidas: %{y} (<b>%{customdata[2]}%</b> del total)<br>
                    Winrate: <b>%{customdata[0]}%</b><br>
                    Lossrate: <b>%{customdata[1]}%</b>
                    <extra></extra>
                    """
                ),
                row=1, col=1
            )
            
            # 2. Matrix (Bottom)
            y_labels = list(obj_names.values())
            scatter_x, scatter_y, scatter_color = [], [], []
            
            for i, comb in enumerate(comb_stats['comb']):
                active_objs = comb.split(" + ")
                for obj in y_labels:
                    scatter_x.append(comb)
                    scatter_y.append(obj)
                    scatter_color.append('#3B82F6' if obj in active_objs else 'rgba(200,200,200,0.1)')
                    
            fig.add_trace(
                go.Scatter(
                    x=scatter_x,
                    y=scatter_y,
                    mode='markers',
                    marker=dict(size=16, color=scatter_color, line=dict(width=1, color='white')),
                    showlegend=False,
                    hoverinfo='skip'
                ),
                row=2, col=1
            )
            
            for i, comb in enumerate(comb_stats['comb']):
                active_objs = comb.split(" + ")
                if len(active_objs) > 1:
                    active_indices = [y_labels.index(o) for o in active_objs]
                    fig.add_trace(
                        go.Scatter(
                            x=[comb, comb],
                            y=[y_labels[min(active_indices)], y_labels[max(active_indices)]],
                            mode='lines',
                            line=dict(color='#3B82F6', width=3),
                            showlegend=False,
                            hoverinfo='skip'
                        ),
                        row=2, col=1
                    )

            fig.update_layout(
                height=750,
                showlegend=False,
                paper_bgcolor=PAPER,
                plot_bgcolor="rgba(0,0,0,0)",
                font=dict(family="Inter, sans-serif", color=TEXT, size=12),
                margin=dict(l=100, r=40, t=20, b=40)
            )
            
            # Quitar las etiquetas del eje X para que no se solapen (la matriz es la etiqueta)
            fig.update_xaxes(showticklabels=False, row=1, col=1)
            fig.update_xaxes(showticklabels=False, row=2, col=1)
            
            fig.update_yaxes(title_text="Partidas", row=1, col=1, gridcolor="rgba(255,255,255,0.05)")
            fig.update_yaxes(gridcolor="rgba(255,255,255,0.05)", row=2, col=1)
            
            st.plotly_chart(fig, use_container_width=True)

    with tab_evolution:
        st.markdown("### Evolución de Winrate")
        st.markdown(
            f"<p style='color:{MUTED}; margin-bottom:20px;'>"
            "Visualiza la trayectoria del winrate en el tiempo. Selecciona la granularidad para agrupar partidas y filtra por jugador para comparar rendimientos."
            "</p>", 
            unsafe_allow_html=True
        )

        df_evo = get_winrate_evolution_data(pool_id, queue_id, min_friends)
        
        if df_evo.empty:
            st.warning("No hay datos suficientes para mostrar la evolución.")
        else:
            # Selectores
            c1, c2, c3 = st.columns([1, 1, 2])
            with c1:
                granularity = st.selectbox(
                    "Granularidad (agrupar cada):",
                    ["Diario", "3 días", "Semanal"],
                    index=1 # 3 días por defecto
                )
            
            with c2:
                min_games_threshold = st.number_input(
                    "Empezar desde partida nº:",
                    min_value=1,
                    max_value=50,
                    value=10,
                    help="El winrate acumulado es muy volátil al principio. Este filtro oculta los primeros datos para ver tendencias estables."
                )
            
            freq_map = {"Diario": "1D", "3 días": "3D", "Semanal": "7D"}
            freq = freq_map[granularity]

            all_personas = sorted(df_evo["persona"].unique().tolist())
            with c3:
                selected_personas = st.multiselect(
                    "Seleccionar jugadores:",
                    ["Toda la Comunidad"] + all_personas,
                    default=["Toda la Comunidad"]
                )

            # Procesamiento de datos
            df_evo["game_start_at"] = pd.to_datetime(df_evo["game_start_at"])
            
            lines_to_plot = []

            # 1. Calcular Comunidad si se solicita
            if "Toda la Comunidad" in selected_personas:
                df_comm = df_evo.copy().sort_values("game_start_at")
                df_comm["win_val"] = df_comm["win"].astype(int)
                df_comm["cum_wins"] = df_comm["win_val"].cumsum()
                df_comm["cum_total"] = range(1, len(df_comm) + 1)
                df_comm["cum_winrate"] = (df_comm["cum_wins"] / df_comm["cum_total"]) * 100
                
                # Filtrar por umbral
                df_comm = df_comm[df_comm["cum_total"] >= min_games_threshold]
                
                if not df_comm.empty:
                    df_comm = df_comm.set_index("game_start_at")
                    # Resampleamos tomando el último valor del periodo (el winrate en ese momento)
                    grouped = df_comm["cum_winrate"].resample(freq).last().ffill()
                    games_count = df_comm["cum_total"].resample(freq).last().ffill()
                    
                    lines_to_plot.append({
                        "name": "Comunidad",
                        "x": grouped.index,
                        "y": grouped,
                        "color": GOLD,
                        "width": 4,
                        "games": games_count
                    })

            # 2. Calcular por Persona
            for p in selected_personas:
                if p == "Toda la Comunidad": continue
                df_p = df_evo[df_evo["persona"] == p].copy().sort_values("game_start_at")
                df_p["win_val"] = df_p["win"].astype(int)
                df_p["cum_wins"] = df_p["win_val"].cumsum()
                df_p["cum_total"] = range(1, len(df_p) + 1)
                df_p["cum_winrate"] = (df_p["cum_wins"] / df_p["cum_total"]) * 100
                
                # Filtrar por umbral
                df_p = df_p[df_p["cum_total"] >= min_games_threshold]
                
                if not df_p.empty:
                    df_p = df_p.set_index("game_start_at")
                    grouped = df_p["cum_winrate"].resample(freq).last().ffill()
                    games_count = df_p["cum_total"].resample(freq).last().ffill()
                    
                    lines_to_plot.append({
                        "name": p,
                        "x": grouped.index,
                        "y": grouped,
                        "width": 2,
                        "games": games_count
                    })

            # Renderizado con Plotly
            fig_evo = go.Figure()
            
            for line in lines_to_plot:
                fig_evo.add_trace(go.Scatter(
                    x=line["x"],
                    y=line["y"],
                    name=line["name"],
                    mode="lines+markers",
                    line=dict(width=line.get("width", 2), shape="spline"),
                    marker=dict(size=6),
                    customdata=line["games"],
                    hovertemplate=(
                        f"<b>{line['name']}</b><br>"
                        "Fecha: %{x|%Y-%m-%d}<br>"
                        "Winrate: %{y:.1f}%<br>"
                        "Partidas: %{customdata}<extra></extra>"
                    )
                ))

            fig_evo.update_layout(
                paper_bgcolor=BG,
                plot_bgcolor=BG,
                hovermode="closest",
                height=500,
                margin=dict(l=40, r=40, t=40, b=40),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1,
                    font=dict(color=TEXT)
                ),
                xaxis=dict(
                    showgrid=False,
                    color=TEXT,
                    tickfont=dict(size=10),
                    gridcolor=BORDER
                ),
                yaxis=dict(
                    showgrid=True,
                    gridcolor=BORDER,
                    color=TEXT,
                    range=[40, 80],
                    tickformat=".0f",
                    title=dict(text="Winrate %", font=dict(size=12, color=MUTED))
                )
            )
            
            # Línea del 50%
            fig_evo.add_hline(y=50, line_dash="dash", line_color=BORDER, opacity=0.8)

            st.plotly_chart(fig_evo, use_container_width=True, theme=None)
            st.write("---")
