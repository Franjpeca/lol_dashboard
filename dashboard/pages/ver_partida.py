"""
dashboard/pages/ver_partida.py
Sección: Ver Partida — últimas partidas del grupo con visualización de equipos.
"""
import streamlit as st
from dashboard.db import get_recent_matches, get_match_detail
from dashboard.theme import BG, BORDER, GOLD, MUTED, GREEN, RED

# ── Helpers de DDragon (reutilizados de example/viewGame/dragon.py) ───────────
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from example.viewGame.dragon import (
    get_latest_patch,
    champion_square_url,
    spell_icon_url,
    rune_icon_url,
    rune_style_icon_url,
    item_icon_url,
)

# ── CSS inyectado una sola vez ────────────────────────────────────────────────
_MATCH_CSS = """
<style>
.match-team-header {
    font-size: 0.85rem; font-weight: 700; letter-spacing: .5px;
    padding: 6px 0 6px 0; margin-bottom: 4px;
}
.match-win  { color: #3ba55d; }
.match-lose { color: #c0392b; }
.team-blue  { color: #3498db; }
.team-red   { color: #e74c3c; }
div[data-testid="stExpander"]:has(.expander-win) details {
    border: 1px solid rgba(59, 165, 93, 0.3);
    border-left: 6px solid #3ba55d;
}
div[data-testid="stExpander"]:has(.expander-lose) details {
    border: 1px solid rgba(192, 57, 43, 0.3);
    border-left: 6px solid #c0392b;
}
.match-table {
    width: 100%; border-collapse: collapse; font-size: 0.85rem;
    margin-bottom: 20px; table-layout: fixed;
}
.match-table th {
    color: #888; font-weight: 600; padding: 6px 4px;
    border-bottom: 1px solid rgba(255,255,255,.10); text-align:center;
}
.match-table td {
    padding: 6px 4px; border-bottom: 1px solid rgba(255,255,255,.06);
    vertical-align: middle; text-align:center;
}
.match-table tr:last-child td { border-bottom: none; }
.match-champ-icon {
    width: 40px; height: 40px; border-radius: 50%;
    border: 2px solid rgba(255,255,255,.15); object-fit: cover;
}
.match-item-icon {
    width: 28px; height: 28px; border-radius: 4px;
    border: 1px solid rgba(255,255,255,.10); object-fit: cover;
    margin-right: 3px;
}
.match-spell-icon {
    width: 24px; height: 24px; border-radius: 3px;
    border: 1px solid rgba(255,255,255,.10); object-fit: cover;
    margin-bottom: 3px;
}
.match-rune-icon {
    width: 24px; height: 24px; object-fit: contain;
    margin-bottom: 3px;
}
.dmg-bar-wrap { display:flex; align-items:center; justify-content:center; gap:8px; }
.dmg-bar {
    height: 7px; border-radius: 3.5px; background: #e74c3c;
    min-width: 4px;
}
.champ-level {
    font-size: 0.72rem; color: #888; display: block;
}
.friend-name { color: #c9aa71; font-weight: 600; font-size: 0.9rem; }
.small-stat { color: #888; font-size: 0.75rem; }
</style>
"""


def _img(url, css_class, title=""):
    if not url:
        return ""
    return f'<img src="{url}" class="{css_class}" title="{title}" onerror="this.style.display=\'none\'">'


def _render_player_row(p, patch, is_header=False):
    champ = p.get("champ", "")
    name  = p.get("name", "")
    k, d, a = p.get("kills", 0), p.get("deaths", 0), p.get("assists", 0)
    kda   = p.get("kda", 0.0)
    dmg   = p.get("damage", 0)
    dmg_share = p.get("damageShare", 0)
    gold  = p.get("gold", 0)
    cs    = p.get("cs", 0)
    cspm  = p.get("cspm", 0)
    vs    = p.get("visionScore", 0)
    items = p.get("items", [])

    # ── Iconos de campeón ─────────────────────────────────────────────────────
    champ_img = _img(champion_square_url(champ, patch), "match-champ-icon", champ)
    level_span = f'<span class="champ-level">Nv. {p.get("champLevel",1)}</span>'

    # ── Hechizos ──────────────────────────────────────────────────────────────
    s1_img = _img(spell_icon_url(p.get("summoner1Id"), patch), "match-spell-icon")
    s2_img = _img(spell_icon_url(p.get("summoner2Id"), patch), "match-spell-icon")
    spells_html = f'<div style="display:flex;flex-direction:column;">{s1_img}{s2_img}</div>'

    # ── Runas ─────────────────────────────────────────────────────────────────
    r1_img = _img(rune_icon_url(p.get("primary")), "match-rune-icon")
    r2_img = _img(rune_style_icon_url(p.get("secondary")), "match-rune-icon")
    runes_html = f'<div style="display:flex;flex-direction:column;">{r1_img}{r2_img}</div>'

    # ── Ítems ─────────────────────────────────────────────────────────────────
    items_html = "<div style='display:flex; justify-content:center;'>" + "".join(
        _img(item_icon_url(it, patch), "match-item-icon", str(it))
        for it in items if it and it != 0
    ) + "</div>"

    # ── Nombre (resaltado si tiene alias en el mapa) ───────────────────────────
    name_cell = f'<span class="friend-name">{name}</span>'

    # ── Barra de daño ─────────────────────────────────────────────────────────
    bar_w = max(4, int(dmg_share * 1.5)) # multiplicador visual para que no queden muy pequeñas
    dmg_cell = (
        f'<div class="dmg-bar-wrap">'
        f'<div style="min-width:55px; text-align:right;">{dmg:,}</div>'
        f'<div class="dmg-bar" style="width:{bar_w}px;"></div>'
        f'<span class="small-stat" style="min-width:35px; text-align:left;">{dmg_share}%</span>'
        f'</div>'
    )

    return (
        f"<tr>"
        f"<td><div style='display:flex;align-items:center;justify-content:center;gap:8px;'>"
        f"  {champ_img}{level_span}"
        f"  <div style='display:flex;gap:3px;'>{spells_html}{runes_html}</div>"
        f"</div></td>"
        f"<td>{name_cell}<br><span class='small-stat'>{p.get('role','')}</span></td>"
        f"<td><b>{k}/{d}/{a}</b><br><span class='small-stat'>KDA {kda}</span></td>"
        f"<td>{dmg_cell}</td>"
        f"<td>{gold:,}<br><span class='small-stat'>{p.get('gpm',0)} /min</span></td>"
        f"<td>{cs}<br><span class='small-stat'>{cspm} /min</span></td>"
        f"<td style='color:#a8d8ea'>{vs}</td>"
        f"<td>{items_html}</td>"
        f"</tr>"
    )


def _render_team(team_key, team_data, patch):
    win  = team_data.get("win", False)
    players = team_data.get("players", [])
    
    # Textos
    result  = "VICTORIA" if win else "DERROTA"
    name    = "Equipo Azul" if team_key == "blue" else "Equipo Rojo"
    
    # Clases CSS para color
    name_cls   = "team-blue" if team_key == "blue" else "team-red"
    result_cls = "match-win" if win else "match-lose"

    header = f'<div class="match-team-header"><span class="{name_cls}">{name}</span> - <span class="{result_cls}">{result}</span></div>'
    rows   = "".join(_render_player_row(p, patch) for p in players)
    table  = (
        f"<table class='match-table'>"
        # ── Definición de anchos fijos para que todo quede cuadriculado ───────
        f"<colgroup>"
        f"  <col style='width: 15%;'>"  # Campeón
        f"  <col style='width: 15%;'>"  # Jugador
        f"  <col style='width: 10%;'>"  # KDA
        f"  <col style='width: 18%;'>"  # Daño
        f"  <col style='width: 10%;'>"  # Oro
        f"  <col style='width: 8%;'>"   # CS
        f"  <col style='width: 6%;'>"   # VS
        f"  <col style='width: 18%;'>"  # Ítems
        f"</colgroup>"
        f"<thead><tr>"
        f"  <th>Campeón</th><th>Jugador</th><th>KDA</th>"
        f"  <th>Daño</th><th>Oro</th><th>CS</th><th>VS</th><th>Ítems</th>"
        f"</tr></thead>"
        f"<tbody>{rows}</tbody>"
        f"</table>"
    )
    return header + table


def render(pool_id: str, queue_id: int, min_friends: int):
    st.header("Ver partidas")

    from dashboard.db import get_matches_filtered, get_all_personas, get_match_detail

    # ── Controles de filtrado ─────────────────────────────────────────────────
    col_id, col_fecha, col_persona, col_slider = st.columns([3, 2, 2, 2])

    with col_id:
        match_id_search = st.text_input("Buscar por ID de partida", placeholder="EUW1_78…", key="vp_id_search")

    with col_fecha:
        date_val = st.date_input("Filtrar por día", value=None, key="vp_date")
        date_filter = str(date_val) if date_val else ""

    with col_persona:
        personas_list = get_all_personas(pool_id, queue_id, min_friends)
        persona_options = ["Todos"] + personas_list
        persona_sel     = st.selectbox("Filtrar por persona", persona_options, key="vp_persona")
        persona_filter  = "" if persona_sel == "Todos" else persona_sel

    with col_slider:
        limit = st.slider("Nº partidas", 5, 200, 20, step=5, key="vp_limit")

    # Si se escribe ID, ignoramos los demás filtros para búsqueda puntual
    if match_id_search:
        date_filter    = ""
        persona_filter = ""

    df = get_matches_filtered(
        pool_id, queue_id, min_friends,
        limit=limit,
        match_id_search=match_id_search,
        date_filter=date_filter,
        persona_filter=persona_filter,
    )

    if df.empty:
        st.info("No se encontraron partidas con los filtros seleccionados.")
        return

    st.markdown(f"*{len(df)} partida(s) encontrada(s)*")

    patch = get_latest_patch()
    st.markdown(_MATCH_CSS, unsafe_allow_html=True)

    for _, row in df.iterrows():
        personas     = ", ".join(sorted(row.get("personas_present") or []))
        duration_min = int((row.get("duration_s") or 0) // 60)
        duration_sec = int((row.get("duration_s") or 0) % 60)
        start        = str(row.get("game_start_at", ""))[:16] if row.get("game_start_at") else "-"
        match_id     = row["match_id"]
        group_win    = row.get("group_win")

        label = f"{match_id}  -  {start}  ({duration_min}:{duration_sec:02d})  · {personas or 'Sin datos'}"

        with st.expander(label):
            # Inyectar el trigger para colorear el expander vía CSS
            marker_cls = "expander-win" if group_win else "expander-lose"
            st.markdown(f"<div class='{marker_cls}' style='display:none;'></div>", unsafe_allow_html=True)

            with st.spinner("Cargando detalle de la partida…"):
                detail = get_match_detail(match_id)

            if "error" in detail:
                st.warning(f"No se pudo cargar el detalle: {detail['error']}")
                st.markdown(f"**Jugadores presentes:** {personas or 'Sin datos'}")
                st.markdown(f"**Duración:** {duration_min} min {duration_sec} seg")
                continue

            # ── Render de los dos equipos ─────────────────────────────────
            blue_html = _render_team("blue", detail["teams"]["blue"], patch)
            red_html  = _render_team("red",  detail["teams"]["red"],  patch)
            st.markdown(blue_html + "<br>" + red_html, unsafe_allow_html=True)

