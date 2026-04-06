"""
run_all.py
==========
Script maestro de actualización del Dashboard.

Flujo completo:
  [1] Ingesta L0: Actualiza índices de usuarios (todas las pools) y descarga nuevas partidas
  [2] ETL L1→PG: Para CADA pool detectada × CADA nivel de min_friends (1-5):
      - build_L1_filtered.py → genera la colección Mongo filtrada
      - populate_pg.py → vuelca a PostgreSQL

La detección de pools es automática: cualquier archivo data/mapa_cuentas*.json crea una pool.
La colección de usuarios se auto-detecta en build_L1_filtered y populate_pg (gracias al fix),
pero aquí la pasamos explícitamente para mayor robustez y trazabilidad.

Uso:
    python src/run_all.py                  # Descarga las últimas 15 partidas por jugador
    python src/run_all.py --limit 100      # Descarga las últimas 100
    python src/run_all.py --all            # Descarga TODO el historial (muy lento)
    python src/run_all.py --skip-l0        # Solo regenera ETL, sin descargar partidas nuevas
"""

import sys
import datetime
import subprocess
import argparse
import json
import psycopg2
from pathlib import Path

# Rutas base
FILE_SELF = Path(__file__).resolve()
ROOT      = FILE_SELF.parents[1]   # lol_dashboard/
SRC_DIR   = ROOT / "src"
DATA_DIR  = ROOT / "data"

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from utils.config import POSTGRES_URI
from utils.status import save_last_update

_PG_DSN = POSTGRES_URI.replace("postgresql+psycopg2://", "postgresql://")


# ──────────────────────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────────────────────

def run_cmd(*args, label: str = "") -> bool:
    """Ejecuta un script de Python y devuelve True si tuvo éxito."""
    cmd = [sys.executable] + [str(a) for a in args]
    tag = label or Path(args[0]).stem
    print(f"\n{'─'*60}")
    print(f"  [RUN] {tag}")
    print(f"  CMD : {' '.join(cmd)}")
    print(f"{'─'*60}", flush=True)
    result = subprocess.run(cmd, cwd=ROOT)
    if result.returncode != 0:
        print(f"\n  ❌ ERROR en '{tag}' (exit code {result.returncode})", flush=True)
        return False
    print(f"\n  ✅ OK: {tag}", flush=True)
    return True


def discover_pools() -> list[dict]:
    """
    Escanea data/ y devuelve la lista de pools a procesar.
    Cada pool es un dict con las claves:
      - pool_id       : str  → ID que se usará en PostgreSQL y Mongo L1
      - users_coll    : str  → nombre de la colección Mongo L0_users_index_*
      - date_filter   : bool → True si hay que aplicar filtro temporal (ej. season)
    """
    pools = []

    # ── Pool base: villaquesitos ──────────────────────────────────────────────
    if (DATA_DIR / "mapa_cuentas.json").exists():
        pools.append({
            "pool_id":     "villaquesitos",
            "users_coll":  "L0_users_index",
            "date_filter": False,
        })

    # ── Pools dinámicas: mapa_cuentas_XXX.json ───────────────────────────────
    for f in sorted(DATA_DIR.glob("mapa_cuentas_*.json")):
        suffix = f.stem.replace("mapa_cuentas_", "")
        pools.append({
            "pool_id":     suffix,
            "users_coll":  f"L0_users_index_{suffix}",
            "date_filter": (suffix == "season"),
        })

    return pools


def ensure_pools_in_db(pools: list[dict], min_friends_range: range):
    """Pre-crea registros en la tabla `pools` de PostgreSQL para evitar errores FK en el ETL."""
    try:
        conn = psycopg2.connect(_PG_DSN)
        cur = conn.cursor()

        # Limpiar IDs con espacios residuales
        cur.execute("DELETE FROM pools WHERE pool_id LIKE '% %'")

        for p in pools:
            for m in min_friends_range:
                cur.execute("""
                    INSERT INTO pools (pool_id, queue_id, min_friends)
                    VALUES (%s, 0, %s)
                    ON CONFLICT DO NOTHING
                """, (p["pool_id"], m))
        conn.commit()
        cur.close()
        conn.close()
        print("  ✅ Pools aseguradas en PostgreSQL")
    except Exception as e:
        print(f"  ⚠️  No se pudo conectar a PostgreSQL para pre-crear pools: {e}")
        print("     (Continúa de todas formas; el ETL intentará crearlas)")


# ──────────────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Actualización total del Dashboard (Ingesta + ETL para todas las pools)"
    )
    parser.add_argument("--limit",   type=int,  default=15,
                        help="Partidas máximas a descargar por jugador (defecto 15)")
    parser.add_argument("--all",     action="store_true",
                        help="Descargar TODO el historial (ignora --limit)")
    parser.add_argument("--skip-l0", action="store_true",
                        help="Omitir descarga de partidas (solo regenera ETL)")
    parser.add_argument("--min-from", type=int, default=1,
                        help="Nivel mínimo de min_friends a generar (defecto 1)")
    parser.add_argument("--min-to",   type=int, default=5,
                        help="Nivel máximo de min_friends a generar (defecto 5)")
    args = parser.parse_args()

    min_range = range(args.min_from, args.min_to + 1)

    # ── Banner ────────────────────────────────────────────────────────────────
    now = datetime.datetime.now(datetime.timezone.utc)
    print("\n" + "=" * 60)
    print("  🔥 ACTUALIZACIÓN TOTAL DEL DASHBOARD 🔥")
    print(f"  {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Modo L0 : {'OMITIDO (--skip-l0)' if args.skip_l0 else ('COMPLETO (--all)' if args.all else f'RÁPIDO (--limit {args.limit})')}")
    print(f"  Niveles : min_friends {args.min_from}..{args.min_to}")
    print("=" * 60, flush=True)

    # ── Detectar pools ────────────────────────────────────────────────────────
    pools = discover_pools()
    print(f"\n📋 Pools detectadas: {[p['pool_id'] for p in pools]}")

    # ── Paso 0: Pre-crear pools en PostgreSQL ─────────────────────────────────
    print("\n" + "=" * 60)
    print("  [PASO 0] Asegurando estructura de pools en PostgreSQL")
    print("=" * 60)
    ensure_pools_in_db(pools, min_range)

    # ── Paso 1: Actualizar índices de usuarios (L0) ───────────────────────────
    print("\n" + "=" * 60)
    print("  [PASO 1] Actualizando índices de usuarios (todas las pools)")
    print("=" * 60)
    run_cmd(
        SRC_DIR / "extract" / "ingest_users.py", "--mode", "all",
        label="ingest_users --mode all"
    )

    # ── Paso 2: Descargar nuevas partidas (L0) ────────────────────────────────
    if not args.skip_l0:
        print("\n" + "=" * 60)
        print("  [PASO 2] Descargando partidas desde Riot API")
        print("=" * 60)
        # ingest_matches auto-detecta TODAS las L0_users_index* de Mongo
        extra = ["--all"] if args.all else ["--limit", str(args.limit)]
        run_cmd(
            SRC_DIR / "extract" / "ingest_matches.py", *extra,
            label=f"ingest_matches {'--all' if args.all else f'--limit {args.limit}'}"
        )
    else:
        print("\n  [PASO 2] ⏭️  Omitido (--skip-l0)")

    # ── Paso 3: ETL L1→PG para cada pool × min_friends ──────────────────────
    print("\n" + "=" * 60)
    print(f"  [PASO 3] ETL: {len(pools)} pools × {len(min_range)} niveles min_friends")
    print("=" * 60)

    total = len(pools) * len(min_range)
    done  = 0
    errors = []

    for pool in pools:
        pid        = pool["pool_id"]
        users_coll = pool["users_coll"]

        print(f"\n  ┌─ POOL: {pid.upper()} (users_collection={users_coll})")

        for m in min_range:
            done += 1
            print(f"\n  │  [{done}/{total}] min_friends={m}")

            # build_L1_filtered.py
            ok = run_cmd(
                SRC_DIR / "load" / "build_L1_filtered.py",
                "--pool", pid,
                "--min",  str(m),
                "--users-collection", users_coll,
                label=f"L1 filter  pool={pid} min={m}"
            )
            if not ok:
                errors.append(f"build_L1_filtered pool={pid} min={m}")
                continue

            # populate_pg.py
            ok = run_cmd(
                SRC_DIR / "load" / "populate_pg.py",
                "--pool", pid,
                "--min",  str(m),
                "--users-collection", users_coll,
                label=f"ETL → PG   pool={pid} min={m}"
            )
            if not ok:
                errors.append(f"populate_pg pool={pid} min={m}")

        print(f"  └─ Pool '{pid}' finalizada")

    # ── Resumen ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    if errors:
        print(f"  ⚠️  PROCESO COMPLETADO CON {len(errors)} ERROR(ES):")
        for e in errors:
            print(f"     - {e}")
    else:
        print("  ✅ PROCESO COMPLETADO EXITOSAMENTE")
    print("=" * 60)

    save_last_update()


if __name__ == "__main__":
    main()
