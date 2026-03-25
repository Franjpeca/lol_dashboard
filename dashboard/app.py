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
    ("Datos y configuración",    "", "dashboard.pages.config"),
]

# ─── Top Navbar ───────────────────────────────────────────────────────────────
# We use st.columns to embed selectboxes in the navbar row alongside static HTML.

pool_options = get_pool_options()
# Si está vacío por lo que sea, ponemos defaults de salvaguarda
if not pool_options:
    pool_options = {"ca879f16": [5]}

pools = list(pool_options.keys())

nav_left, nav_pool_label, nav_pool, nav_friends_label, nav_friends, nav_status, nav_spacer = st.columns(
    [1.2, 0.4, 1.1, 0.3, 0.8, 0.9, 2.5]
)

queue_id = 440  # Solo Flex

with nav_left:
    st.markdown(
        f"<div style='padding-top:2px; padding-bottom:10px;'><div class='lol-brand-gradient'>Villaquesitos.gg</div></div>",
        unsafe_allow_html=True,
    )

with nav_spacer:
    pass

with nav_pool_label:
    st.markdown(
        f"<div style='padding-top:14px; color:{MUTED}; font-size:0.9rem; "
        f"font-weight:600; text-transform:uppercase; letter-spacing:.5px;'>Pool:</div>",
        unsafe_allow_html=True,
    )

with nav_pool:
    def format_pool(pid):
        if pid == "season":
            return "Season"
        return f"Normal ({pid})"

    pool_id = st.selectbox(
        "pool",
        pools,
        format_func=format_pool,
        label_visibility="collapsed",
    )

with nav_friends_label:
    st.markdown(
        f"<div style='padding-top:14px; color:{MUTED}; font-size:0.9rem; "
        f"font-weight:600; text-transform:uppercase; letter-spacing:.5px;'>Min:</div>",
        unsafe_allow_html=True,
    )

with nav_friends:
    valid_mins = sorted(pool_options.get(pool_id, [5]))
    min_friends = st.selectbox(
        "min_friends",
        valid_mins,
        index=len(valid_mins)-1, # Por defecto el mayor (ej. 5)
        label_visibility="collapsed",
    )

with nav_status:
    st.markdown(
        f"<div style='padding-top:11px; text-align:right;'>"
        f"<span class='lol-badge'>Activo</span></div>",
        unsafe_allow_html=True,
    )

with nav_spacer:
    pass

# Thin separator line under navbar
st.markdown(
    f"<hr style='border:none; border-top:1px solid {BORDER}; margin:0 0 0 0;'>",
    unsafe_allow_html=True,
)

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
