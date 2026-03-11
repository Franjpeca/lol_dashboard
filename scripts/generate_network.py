import json
import sys
import argparse
from pathlib import Path
import networkx as nx
import plotly.graph_objects as go
from itertools import combinations
import math

def extract_players_from_match(match_data):
    """
    Extracts participants from a Match-V5 JSON.
    Returns a list of dicts: [{'name': str, 'win': bool}, ...]
    """
    try:
        if "data" in match_data and "info" in match_data["data"]:
            participants = match_data["data"]["info"].get("participants", [])
        elif "info" in match_data:
            participants = match_data["info"].get("participants", [])
        else:
            return []
            
        players = []
        for p in participants:
            riot_id = p.get("riotIdGameName")
            tag = p.get("riotIdTagline") or p.get("riotIdTagLine")
            summoner = p.get("summonerName")
            
            if riot_id:
                name = f"{riot_id}#{tag}" if tag else riot_id
            elif summoner:
                name = summoner
            else:
                name = p.get("puuid", "Unknown")[:8]
                
            players.append({
                "name": name,
                "win": p.get("win", False)
            })
        return players
    except Exception as e:
        print(f"Error parsing match: {e}")
        return []

def get_color_for_winrate(wr):
    """Calcula un color entre rojo (0%) y verde (100%) en formato hex."""
    # HSL: Hue de 0 (Rojo) a 120 (Verde)
    # Aproximación RGB simple:
    r = int(255 * (1 - wr))
    g = int(255 * wr)
    return f"rgb({r}, {g}, 50)"

def main():
    parser = argparse.ArgumentParser(description="Generar red de jugadores desde JSONL.")
    parser.add_argument("input_path", help="Archivo JSONL o directorio con archivos JSONL")
    parser.add_argument("--output", default="network_graph.html", help="Archivo HTML de salida")
    parser.add_argument("--min-matches", type=int, default=5, help="Partidas mínimas en dúo para crear arista")
    args = parser.parse_args()

    input_path = Path(args.input_path)
    files_to_process = []
    
    if input_path.is_file():
        files_to_process.append(input_path)
    elif input_path.is_dir():
        files_to_process.extend(input_path.glob("*.jsonl"))
        files_to_process.extend(input_path.glob("*.json")) # Add generic json just in case
    else:
        print(f"No se encontró la ruta: {input_path}")
        sys.exit(1)

    print(f"Procesando {len(files_to_process)} archivo(s)...")

    player_stats = {} 
    duo_stats = {}    

    total_matches = 0
    for file_path in files_to_process:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    match_data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                
                players = extract_players_from_match(match_data)
                if not players:
                    continue
                    
                total_matches += 1
                
                # Nodos
                for p_dict in players:
                    p = p_dict["name"]
                    w = p_dict["win"]
                    if p not in player_stats:
                        player_stats[p] = {"matches": 0, "wins": 0}
                    player_stats[p]["matches"] += 1
                    if w:
                        player_stats[p]["wins"] += 1
                
                # Aristas
                for p1_dict, p2_dict in combinations(players, 2):
                    n1, n2 = p1_dict["name"], p2_dict["name"]
                    w1, w2 = p1_dict["win"], p2_dict["win"]
                    
                    if n1 == n2:
                        continue
                        
                    pair = tuple(sorted([n1, n2]))
                    
                    if pair not in duo_stats:
                        duo_stats[pair] = {"matches": 0, "wins": 0}
                    duo_stats[pair]["matches"] += 1
                    if w1 and w2:
                        duo_stats[pair]["wins"] += 1

    print(f"Total partidas analizadas: {total_matches}")
    if total_matches == 0:
        print("No se encontraron partidas válidas. Saliendo.")
        sys.exit(0)

    # ---------------------------------------------------------
    # Constuir Grafo NetworkX
    # ---------------------------------------------------------
    G = nx.Graph()
    
    # Pre-filtrar nodos (sólo agregamos los que tienen aristas válidas o los queremos todos?)
    # Añadiremos todos los que formen parte de al menos una arista válida
    valid_nodes = set()
    for (p1, p2), stats in duo_stats.items():
        if stats["matches"] >= args.min_matches:
            valid_nodes.add(p1)
            valid_nodes.add(p2)
            
    if not valid_nodes:
        print(f"Ningún dúo tiene {args.min_matches} o más partidas juntos. Prueba a bajar --min-matches.")
        sys.exit(0)

    # Añadir Nodos
    for p in valid_nodes:
        m = player_stats[p]["matches"]
        w = player_stats[p]["wins"]
        wr = w / m if m > 0 else 0
        G.add_node(p, matches=m, winrate=wr)

    # Añadir Aristas
    edge_count = 0
    for (p1, p2), stats in duo_stats.items():
        m = stats["matches"]
        if m >= args.min_matches:
            w = stats["wins"]
            wr = w / m if m > 0 else 0
            G.add_edge(p1, p2, weight=m, winrate=wr, matches=m)
            edge_count += 1
            
    print(f"Generando grafo con {len(valid_nodes)} jugadores y {edge_count} conexiones reales...")

    # Layout Spring
    pos = nx.spring_layout(G, k=0.3, iterations=50, seed=42, weight='weight')

    # ---------------------------------------------------------
    # Plotly Visualization
    # ---------------------------------------------------------
    # 1. Trazas para Aristas (Creamos 1 trace base + 1 trace invisible para Hover para optimizar)
    # Sin embargo, queremos colorear por Winrate. Así que lo ideal es colorear por trace o hacer un scatter de segmentos.
    # Dado que el color es por línea, crearemos un scatter principal con `None` gaps, coloreado por una escala general, 
    # pero Plotly requiere Multi-Trace para múltiples colores en modo "lines".
    
    edge_traces = []
    for edge in G.edges(data=True):
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        m = edge[2]['matches']
        wr = edge[2]['winrate']
        
        # Ancho basado en partidas (escalado logarítmico o sqrt para que no sea inmenso)
        width = max(1, math.sqrt(m) * 0.5)
        color = get_color_for_winrate(wr)
        
        hover = f"{edge[0]} & {edge[1]}<br>Dúo Partidas: {m}<br>Dúo Winrate: {wr*100:.1f}%"
        
        edge_trace = go.Scatter(
            x=[x0, x1, None],
            y=[y0, y1, None],
            line=dict(width=width, color=color),
            hoverinfo='text',
            text=[hover, hover, hover],
            mode='lines',
            showlegend=False,
            opacity=0.7
        )
        edge_traces.append(edge_trace)

    # 2. Trazas para Nodos
    node_x = []
    node_y = []
    node_text = []
    node_size = []
    node_color = []

    for node in G.nodes():
        x, y = pos[node]
        node_x.append(x)
        node_y.append(y)
        
        m = G.nodes[node]['matches']
        wr = G.nodes[node]['winrate']
        
        # Tamaño basado en partidas
        node_size.append(max(10, math.sqrt(m) * 3))
        node_color.append(wr)
        
        node_text.append(f"<b>{node}</b><br>Partidas Topales: {m}<br>Winrate: {wr*100:.1f}%")

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        textfont=dict(color='white', size=10),
        textposition="top center",
        hoverinfo='text',
        hovertext=node_text,
        marker=dict(
            showscale=True,
            colorscale='Turbo', # Turbo = Winrate palette
            cmin=0, cmax=1,
            reversescale=False,
            color=node_color,
            size=node_size,
            colorbar=dict(
                thickness=15,
                title='Winrate ND',
                xanchor='left',
                titleside='right',
                tickformat=".0%"
            ),
            line_width=1.5,
            line_color='black'
        )
    )

    # Opcional: mostrar nombres en los nodos más grandes solo
    node_trace.text = [n if G.nodes[n]['matches'] > 10 else "" for n in G.nodes()]

    # Layout de la figura
    fig = go.Figure(data=edge_traces + [node_trace],
             layout=go.Layout(
                title='<br>Red de Jugadores (League of Legends)',
                titlefont_size=16,
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                annotations=[ dict(
                    text="Cada línea une jugadores en la misma partida.<br>Grosor: partidas juntos | Color: Winrate del Dúo",
                    showarrow=False,
                    xref="paper", yref="paper",
                    x=0.005, y=-0.002 ) ],
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                plot_bgcolor='black',
                paper_bgcolor='black',
                font=dict(color='white')
             ))

    # Guardar
    fig.write_html(args.output)
    print(f"¡Gráfico guardado interactivo en {args.output}!")

if __name__ == "__main__":
    main()
