"""
dashboard/pages/config.py
Sección: Datos y Configuración
"""
import streamlit as st
from dashboard.db import _q


def render(pool_id: str, queue_id: int, min_friends: int):
    st.header("Datos y configuración")

    st.subheader("Estado de la base de datos")

    matches_count = _q("SELECT COUNT(*) AS n FROM matches WHERE pool_id=%s AND queue_id=%s AND min_friends=%s",
                       (pool_id, queue_id, min_friends))
    pp_count = _q("SELECT COUNT(*) AS n FROM player_performances WHERE pool_id=%s AND queue_id=%s AND min_friends=%s",
                  (pool_id, queue_id, min_friends))
    friends_count = _q("SELECT COUNT(DISTINCT persona) AS n FROM player_performances WHERE is_friend=TRUE AND pool_id=%s AND queue_id=%s AND min_friends=%s",
                       (pool_id, queue_id, min_friends))

    c1, c2, c3 = st.columns(3)
    c1.metric("Partidas cargadas", int(matches_count["n"].iloc[0]) if not matches_count.empty else 0)
    c2.metric("Registros de jugadores", int(pp_count["n"].iloc[0]) if not pp_count.empty else 0)
    c3.metric("Jugadores activos", int(friends_count["n"].iloc[0]) if not friends_count.empty else 0)

    st.subheader("Actualizar datos")
    st.info("""
Para actualizar los datos del dashboard, ejecuta desde la raíz del proyecto:
```bash
python src/pipeline.py --mode l1-l2 --run-in-terminal
```
Esto volverá a filtrar las partidas de MongoDB y las cargará en PostgreSQL.
Las métricas se recalculan automáticamente por las vistas SQL.
    """)

    st.subheader("Pools disponibles")
    pools_df = _q("SELECT pool_id, COUNT(*) as partidas, MIN(game_start_at) as desde, MAX(game_start_at) as hasta FROM matches GROUP BY pool_id ORDER BY partidas DESC")
    if not pools_df.empty:
        st.dataframe(pools_df, use_container_width=True)
