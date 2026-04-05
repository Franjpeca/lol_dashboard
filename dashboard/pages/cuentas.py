"""
dashboard/pages/cuentas.py
Sección: Información de cuentas
Muestra el mapeo de personas y sus cuentas de LoL.
"""
import streamlit as st
import json
from pathlib import Path
from dashboard.theme import (
    BG, BORDER, TEXT, MUTED, GOLD, PAPER
)
from dashboard.db import _q

def render(pool_id: str, queue_id: int, min_friends: int):
    st.header("Información de cuentas")
    
    # Ruta al archivo de mapeo (Dinámica por Pool)
    ROOT = Path(__file__).resolve().parents[2]
    
    # Intentar cargar el específico de la pool, si no el genérico, si no el de season
    mapa_filename = f"mapa_cuentas_{pool_id.lower()}.json"
    mapa_path = ROOT / "data" / mapa_filename
    
    if not mapa_path.exists():
        # Fallback 1: mapa_cuentas.json
        mapa_path = ROOT / "data" / "mapa_cuentas.json"
    
    if not mapa_path.exists():
        # Fallback 2: mapa_cuentas_season.json (por retrocompatibilidad)
        mapa_path = ROOT / "data" / "mapa_cuentas_season.json"

    if not mapa_path.exists():
        st.error(f"No se ha encontrado ningún archivo de mapeo para la pool '{pool_id}' (buscado: {mapa_filename})")
        return
        
    try:
        with open(mapa_path, "r", encoding="utf-8") as f:
            mapa = json.load(f)
    except Exception as e:
        st.error(f"Error cargando el archivo JSON `{mapa_path.name}`: `{e}`")
        return

    # Buscador
    search_query = st.text_input("🔍 Buscar persona o cuenta...", "").lower()
    
    # Filtrar mapa
    filtered_mapa = {}
    for persona, cuentas in mapa.items():
        # Si la persona coincide
        if search_query in persona.lower():
            filtered_mapa[persona] = cuentas
            continue
        # Si alguna cuenta coincide
        matching_cuentas = [c for c in cuentas if search_query in c.lower()]
        if matching_cuentas:
            filtered_mapa[persona] = cuentas

    if not filtered_mapa:
        st.info("No se han encontrado resultados para tu búsqueda.")
        return

    # --- RECUPERAR ÚLTIMAS PARTIDAS DE POSTGRESQL ---
    # Obtenemos la última partida de cada cuenta (riot_id_name) que haya pasado los filtros de L1/L2
    df_last = _q("""
        SELECT riot_id_name, MAX(game_start_at) as last_match
        FROM player_performances
        WHERE riot_id_name IS NOT NULL
        GROUP BY riot_id_name
    """)
    
    last_matches = {}
    if not df_last.empty:
        for _, row in df_last.iterrows():
            rid = row["riot_id_name"]
            dt = row["last_match"]
            if dt:
                # El campo ya viene como datetime de postgres
                last_matches[rid] = dt.strftime("%d/%m/%Y %H:%M")

    # Estilos CSS para las tarjetas
    st.markdown(f"""
    <style>
    .account-card {{
        background-color: {PAPER};
        border: 1px solid {BORDER};
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 20px;
        transition: transform 0.2s ease, border-color 0.2s ease;
        height: 100%;
    }}
    .account-card:hover {{
        border-color: {GOLD};
        transform: translateY(-2px);
    }}
    .persona-name {{
        color: {GOLD};
        font-family: 'Outfit', sans-serif;
        font-weight: 800;
        font-size: 1.4rem;
        margin-bottom: 12px;
        border-bottom: 1px solid {BORDER};
        padding-bottom: 8px;
    }}
    .account-list {{
        list-style-type: none;
        padding-left: 0;
    }}
    .account-item {{
        color: {TEXT};
        font-size: 0.95rem;
        margin-bottom: 8px;
        display: flex;
        align-items: center;
        justify-content: space-between; /* Riot ID a la izquierda, fecha a la derecha */
        width: 100%;
    }}
    .account-item-name {{
        display: flex;
        align-items: center;
    }}
    .account-item-name::before {{
        content: "•";
        color: {GOLD};
        font-weight: bold;
        display: inline-block;
        width: 1em;
        margin-left: 0.2em;
    }}
    .last-match-date {{
        color: {MUTED};
        font-size: 0.75rem;
        font-weight: 400;
        margin-left: 10px;
        text-align: right;
    }}
    .account-count {{
        color: {MUTED};
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}
    </style>
    """, unsafe_allow_html=True)

    # Mostrar en un grid de columnas
    personas_list = sorted(filtered_mapa.keys())
    
    # Usamos 3 columnas para el grid
    cols = st.columns(3)
    
    for i, persona in enumerate(personas_list):
        with cols[i % 3]:
            cuentas = filtered_mapa[persona]
            
            accounts_html = ""
            for c in cuentas:
                fecha = last_matches.get(c, "Cargando...")
                accounts_html += f"""
                <li class="account-item">
                    <span class="account-item-name">{c}</span>
                    <span class="last-match-date">{fecha}</span>
                </li>
                """
            
            st.markdown(f"""
            <div class="account-card">
                <div class="persona-name">{persona}</div>
                <div class="account-count">{len(cuentas)} CUENTAS:</div>
                <ul class="account-list">
                    {accounts_html}
                </ul>
            </div>
            """, unsafe_allow_html=True)
            st.write("") # Espaciado extra de streamlit
