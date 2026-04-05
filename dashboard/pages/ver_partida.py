"""
dashboard/pages/ver_partida.py
Sección: Ver Partida — últimas partidas del grupo con visualización de equipos.
"""
import streamlit as st
from dashboard.db import get_matches_filtered, get_all_personas, get_match_detail, get_champions_by_persona
from dashboard.theme import BG, BORDER, GOLD, MUTED, GREEN, RED

# ── Helpers de DDragon (ahora locales) ────────────────────────────────────────
from .dragon import (
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
.match-champ-icon {
    width: 40px; height: 40px; border-radius: 50%;
    border: 2px solid rgba(255,255,255,.15); object-fit: cover;
}
.match-item-icon {
    width: 30px; height: 30px; border-radius: 4px;
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


def _render_player_row(p, patch, is_header=False, max_damage: int = 1):
    champ = p.get("champ", "")
    name  = p.get("name", "")
    k, d, a = p.get("kills", 0), p.get("deaths", 0), p.get("assists", 0)
    kda   = p.get("kda", 0.0)
    dmg   = p.get("damage", 0)
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

    # ── Barra de daño (escala respecto al máximo de la partida) ────────────────
    max_w_px = 160
    ratio = (dmg / max_damage) if max_damage and max_damage > 0 else 0
    bar_w = max(6, int(ratio * max_w_px))
    dmg_bar = f'<div class="dmg-bar" style="width:{bar_w}px; background:#e74c3c;"></div>'
    dmg_value = f'<div style="min-width:55px; text-align:right;">{dmg:,}</div>'

    dmg_cell = f'<div class="dmg-bar-wrap" style="justify-content:flex-start; align-items:center; gap:8px;">{dmg_bar}{dmg_value}</div>'

    return (
        f"<tr>"
        # Campeón + nivel + hechizos
        f"<td style='text-align:center;'>"
        f"  <div style='display:flex;align-items:center;justify-content:center;gap:8px;'>"
        f"    {champ_img}{level_span}"
        f"    <div style='display:flex;gap:3px;margin-left:6px;'>{spells_html}{runes_html}</div>"
        f"  </div>"
        f"</td>"
        # Nombre
        f"<td>{name_cell}<br><span class='small-stat'>{p.get('role','')}</span></td>"
        # KDA
        f"<td><b>{k}/{d}/{a}</b><br><span class='small-stat'>KDA {kda}</span></td>"
        # Daño (columna separada, escalar por max_damage)
        f"<td style='text-align:left; padding-left:24px;'>{dmg_cell}</td>"
        # Vision
        f"<td style='min-width:120px; text-align:center; color:#a8d8ea'>{vs}</td>"
        # Oro
        f"<td>{gold:,}<br><span class='small-stat'>{p.get('gpm',0)} /min</span></td>"
        # CS
        f"<td>{cs} CS</td>"
        # Ítems
        f"<td>{items_html}</td>"
        f"</tr>"
    )


def _render_team(team_key, team_data, patch, max_damage: int = 1):
    win  = team_data.get("win", False)
    players = team_data.get("players", [])
    
    # Textos
    result  = "VICTORIA" if win else "DERROTA"
    name    = "Equipo Azul" if team_key == "blue" else "Equipo Rojo"
    
    # Clases CSS para color
    name_cls   = "team-blue" if team_key == "blue" else "team-red"
    result_cls = "match-win" if win else "match-lose"

    header = f'<div class="match-team-header"><span class="{name_cls}">{name}</span> - <span class="{result_cls}">{result}</span></div>'
    rows   = "".join(_render_player_row(p, patch, max_damage=max_damage) for p in players)
    table  = (
        f"<table class='match-table'>"
        # ── Definición de anchos fijos para que todo quede cuadriculado ───────
        f"<colgroup>"
        f"  <col style='width: 12%;'>"  # Campeón
        f"  <col style='width: 13%;'>"  # Jugador
        f"  <col style='width: 9%;'>"   # KDA
        f"  <col style='width: 18%;'>"  # Daño (barra)
        f"  <col style='width: 6%;'>"   # VS
        f"  <col style='width: 11%;'>"  # Oro
        f"  <col style='width: 7%;'>"   # CS
        f"  <col style='width: 24%;'>"  # Ítems
        f"</colgroup>"
        f"<thead><tr>"
        f"  <th>Campeón</th><th>Jugador</th><th>KDA</th>"
        f"  <th>Daño</th><th>VS</th><th>Oro</th><th>CS</th><th>Ítems</th>"
        f"</tr></thead>"
        f"<tbody>{rows}</tbody>"
        f"</table>"
    )
    return header + table


def render(pool_id: str, queue_id: int, min_friends: int):
    st.header("Ver partidas")

    # (Usando importaciones globales ya definidas arriba)

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
        persona_sel     = st.selectbox("Filtrar por persona (Simple)", persona_options, key="vp_persona")
        persona_filter  = "" if persona_sel == "Todos" else persona_sel

    with col_slider:
        limit = st.slider("Nº partidas", 5, 200, 20, step=5, key="vp_limit")

    adv_filters = []
    # Usar session_state para persistir la búsqueda entre interacciones
    if "vp_applied_filters" not in st.session_state:
        st.session_state.vp_applied_filters = None
    
    with st.expander("Filtros Avanzados (Jugador + Campeón + Rol) - AND entre filas", expanded=True):
        st.markdown("<small>Busca partidas donde ocurran TODAS las condiciones (AND). Ej: 'Eduardo con Akali' AND 'Olaf con Azir'.</small>", unsafe_allow_html=True)
        current_adv_filters = []
        for i in range(5):
            c1, c2, c3 = st.columns([1, 1.5, 1.5])
            with c1:
                p_sel = st.selectbox(f"Jugador {i+1}", ["-"] + personas_list, key=f"adv_p_{i}")
            with c2:
                if p_sel != "-":
                    champs_opt = get_champions_by_persona(pool_id, queue_id, min_friends, p_sel)
                    c_sel = st.multiselect(f"Campeones de {p_sel}", champs_opt, key=f"adv_c_{i}")
                else:
                    st.selectbox(f"Campeón {i+1}", ["Selecciona jugador"], disabled=True, key=f"adv_c_dis_{i}")
                    c_sel = []
            with c3:
                if p_sel != "-":
                    r_sel = st.multiselect(f"Roles de {p_sel}", ["TOP", "JUNGLE", "MID", "ADC", "SUPPORT"], key=f"adv_r_{i}")
                    current_adv_filters.append({"persona": p_sel, "champions": c_sel, "roles": r_sel})
                else:
                    st.selectbox(f"Rol {i+1}", ["Selecciona jugador"], disabled=True, key=f"adv_r_dis_{i}")

        st.write("")
        if st.button("🔍 Aplicar Filtros y Buscar", use_container_width=True, type="primary"):
            st.session_state.vp_applied_filters = {
                "limit": limit,
                "date_filter": date_filter,
                "persona_filter": persona_filter,
                "adv_filters": current_adv_filters
            }

    # Determinamos qué filtros usar (si los de sesión o los actuales si hay búsqueda por ID)
    filters_to_use = st.session_state.vp_applied_filters
    
    # Si se escribe ID, ignoramos los demás filtros para búsqueda puntual y forzamos ejecución
    if match_id_search:
        date_filter    = ""
        persona_filter = ""
        adv_filters    = []
        # No usamos el botón, usamos directamente los parámetros
        filters_to_use = {
            "limit": limit,
            "date_filter": "",
            "persona_filter": "",
            "adv_filters": []
        }

    if not filters_to_use:
        st.info("Configura los filtros y pulsa 'Aplicar Filtros y Buscar' para ver los resultados.")
        return

    from dashboard.db import get_matches_filtered
    df = get_matches_filtered(
        pool_id, queue_id, min_friends,
        limit=filters_to_use["limit"],
        match_id_search=match_id_search,
        date_filter=filters_to_use["date_filter"],
        persona_filter=filters_to_use["persona_filter"],
        player_champ_filters=filters_to_use["adv_filters"],
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
            # Escalar barras según el máximo daño en la partida
            blue_players = detail["teams"]["blue"].get("players", [])
            red_players = detail["teams"]["red"].get("players", [])
            all_players = blue_players + red_players
            match_max_damage = max((p.get("damage", 0) for p in all_players), default=1) or 1

            blue_html = _render_team("blue", detail["teams"]["blue"], patch, max_damage=match_max_damage)
            red_html  = _render_team("red",  detail["teams"]["red"],  patch, max_damage=match_max_damage)
            st.markdown(blue_html + "<br>" + red_html, unsafe_allow_html=True)

