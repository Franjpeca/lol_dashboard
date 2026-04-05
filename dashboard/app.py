"""
dashboard/app.py
Entry point del Dashboard LoL — navbar superior + navegación por tabs.

Para añadir una nueva sección:
  1. Crea dashboard/pages/mi_seccion.py con una función render(pool_id, queue_id)
  2. Añade la entrada en SECTIONS abajo
  3. ¡Listo!
"""
import importlib
import sys
import subprocess
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

import streamlit as st

import dashboard.theme
import dashboard.db
importlib.reload(dashboard.theme)
importlib.reload(dashboard.db)

from dashboard.theme import GLOBAL_CSS, GOLD, PAPER, BG, BORDER, MUTED
from dashboard.db import get_pool_options
from utils.status import get_last_update_str

# ─── Page config (must be first Streamlit call) ──────────────────────────────

st.set_page_config(
    page_title="Villaquesitos.gg",
    layout="wide",
    initial_sidebar_state="collapsed",
)
st.markdown(GLOBAL_CSS, unsafe_allow_html=True)

# ─── Section registry ────────────────────────────────────────────────────────

SECTIONS = [
    ("Winrate y partidas",       "", "dashboard.pages.winrate"),
    ("Estadísticas de jugador",  "", "dashboard.pages.stats_jugador"),
    ("Récords",                  "", "dashboard.pages.records"),
    ("Análisis",                 "", "dashboard.pages.analisis"),
    ("Minería",                  "", "dashboard.pages.mineria"),
    ("Ver partida",              "", "dashboard.pages.ver_partida"),
    ("Información de cuentas",   "", "dashboard.pages.cuentas"),
]

pool_options = get_pool_options()
# Si está vacío por lo que sea, ponemos defaults de salvaguarda
if not pool_options:
    pool_options = {"villaquesitos": [5]}

pools = list(pool_options.keys())

# ─── Navigation Header (Redesigned) ──────────────────────────────────────────

queue_id = 440  # Solo Flex

# Usamos 4 columnas principales para equilibrar el espacio
nav_logo, nav_filters, nav_status, nav_btn = st.columns([2.0, 3.2, 2.0, 1.2])

with nav_logo:
    st.markdown(
        "<div class='lol-brand-gradient'>Villaquesitos.gg</div>",
        unsafe_allow_html=True
    )

with nav_filters:
    # Sub-columnas para alinear etiquetas y selectores internamente
    # Agrupamos Pool y Min para que no bailen
    f1, f2, f3, f4 = st.columns([0.4, 1.2, 0.35, 0.7])
    
    with f1:
        st.markdown("<div class='nav-align'><span class='nav-label'>Pool:</span></div>", unsafe_allow_html=True)
    with f2:
        def format_pool(pid):
            if pid == "season": return "Season"
            if pid == "villaquesitos": return "Villaquesitos"
            return pid.capitalize()
        pool_id = st.selectbox("pool", list(pool_options.keys()), format_func=format_pool, label_visibility="collapsed")
        
    with f3:
        st.markdown("<div class='nav-align'><span class='nav-label'>Min:</span></div>", unsafe_allow_html=True)
    with f4:
        valid_mins = sorted(pool_options.get(pool_id, [5]))
        min_friends = st.selectbox("min_friends", valid_mins, index=len(valid_mins)-1, label_visibility="collapsed")

with nav_status:
    last_upd = get_last_update_str()
    st.markdown(
        f"<div class='nav-align' style='justify-content: flex-end;'>"
        f"<div style='text-align:right; color:{MUTED}; font-size:0.8rem; line-height:1.2;'>"
        f"Última actualización: <span style='color:{GOLD}; font-weight:600;'>{last_upd}</span>"
        f"</div></div>",
        unsafe_allow_html=True
    )

with nav_btn:
    if st.button("🔄 Actualizar", key="refresh_btn", use_container_width=True):
        with st.status("🚀 Actualizando datos...", expanded=False) as status:
            try:
                st.write("🔄 Sincronizando con Riot API...")
                subprocess.run([sys.executable, "src/run_all.py"], check=True)
                status.update(label="✅ Actualizado", state="complete")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                status.update(label="❌ Error", state="error")
                st.error(str(e))

st.markdown(f"<hr style='border:none; border-top:1px solid {BORDER}; margin:10px 0 0 0;'>", unsafe_allow_html=True)

# ─── Tab navigation ──────────────────────────────────────────────────────────

tab_labels = [name for name, _, _ in SECTIONS]
tabs = st.tabs(tab_labels)

for tab, (section_name, _, module_path) in zip(tabs, SECTIONS):
    with tab:
        try:
            import dashboard.pages
            page_module = importlib.import_module(module_path)
            importlib.reload(page_module)
            page_module.render(pool_id, queue_id, min_friends)
        except Exception as e:
            st.error(f"Error cargando **{section_name}**: `{e}`")
            import traceback
            st.code(traceback.format_exc())

# ─── Footer ──────────────────────────────────────────────────────────────────
# st.markdown(
#     f"<div style='text-align:center; color:{{MUTED}}; font-size:0.72rem; "
#     f"margin-top:2.5rem; border-top:1px solid {{BORDER}}; padding-top:0.7rem;'>"
#     f"Pool: <code>{{pool_id}}</code> · Min Amigos: <code>{{min_friends}}</code> · Flex · PostgreSQL"
#     f"</div>",
#     unsafe_allow_html=True,
# )
