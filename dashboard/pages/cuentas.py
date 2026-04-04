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

def render(pool_id: str, queue_id: int, min_friends: int):
    st.header("Información de cuentas")
    
    # Ruta al archivo de mapeo
    ROOT = Path(__file__).resolve().parents[2]
    mapa_path = ROOT / "data" / "mapa_cuentas_season.json"
    
    if not mapa_path.exists():
        st.error(f"No se ha encontrado el archivo de mapeo en: `{mapa_path}`")
        return
        
    try:
        with open(mapa_path, "r", encoding="utf-8") as f:
            mapa = json.load(f)
    except Exception as e:
        st.error(f"Error cargando el archivo JSON: `{e}`")
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
        margin-bottom: 6px;
        display: flex;
        align-items: center;
    }}
    .account-item::before {{
        content: "•";
        color: {GOLD};
        font-weight: bold;
        display: inline-block;
        width: 1em;
        margin-left: 0.2em;
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
            accounts_html = "".join([f'<li class="account-item">{c}</li>' for c in cuentas])
            
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
