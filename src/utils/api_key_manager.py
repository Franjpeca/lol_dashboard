import os
import json
from pathlib import Path
from datetime import datetime
from riotwatcher import LolWatcher, RiotWatcher, ApiError

# =====================================================
# DEBUG EXTREMO — ver rutas reales y estructura
# =====================================================

def _log(msg: str):
    print(f"[API_KEY_MANAGER] {msg}")


# Rutas absolutas para debug
FILE_SELF = Path(__file__).resolve()
BASE_DIR = FILE_SELF.parents[2]        # lol_data/
SRC_DIR = FILE_SELF.parents[1]         # src/
UTILS_DIR = FILE_SELF.parents[0]       # utils/

API_KEYS_FILE = BASE_DIR / "data" / "runtime" / "api_keys_temp.json"


_log(f"__file__ = {FILE_SELF}")
_log(f"BASE_DIR = {BASE_DIR}")
_log(f"SRC_DIR = {SRC_DIR}")
_log(f"UTILS_DIR = {UTILS_DIR}")
_log(f"Ruta JSON esperada = {API_KEYS_FILE}")
_log(f"Existe JSON? {API_KEYS_FILE.exists()}")


def _load_all_keys():
    """Carga TODAS las API keys guardadas (lista)."""
    _log(f"Intentando leer archivo: {API_KEYS_FILE}")

    if not API_KEYS_FILE.exists():
        _log("❌ NO SE ENCONTRÓ el archivo.")
        return []

    try:
        raw = API_KEYS_FILE.read_text(encoding="utf-8")
        _log(f"Contenido bruto JSON:\n{raw}")

        data = json.loads(raw)
        _log(f"JSON cargado correctamente: {data}")

        # Formato nuevo
        if "keys" in data:
            _log(f"Cargadas {len(data['keys'])} claves (formato nuevo).")
            return data["keys"]

        # Formato viejo
        if "key" in data:
            _log("Formato viejo detectado. Se convierte.")
            return [{
                "key": data["key"],
                "created_at": data.get("created_at", datetime.utcnow().isoformat())
            }]

        _log("⚠ JSON sin claves.")
        return []

    except Exception as e:
        _log(f"❌ ERROR leyendo JSON: {e}")
        return []


def _save_all_keys(keys: list):
    _log(f"Guardando JSON en {API_KEYS_FILE}")
    API_KEYS_FILE.parent.mkdir(parents=True, exist_ok=True)
    API_KEYS_FILE.write_text(json.dumps({"keys": keys}, indent=2), encoding="utf-8")
    _log(f"✔ Guardadas {len(keys)} claves.")


def save_new_temp_key(key: str):
    _log("Añadiendo nueva API Key...")

    keys = _load_all_keys()
    existing = [k["key"] for k in keys]
    _log(f"Claves existentes: {existing}")

    if key in existing:
        _log("⚠ La clave ya estaba guardada.")
        return

    keys.append({
        "key": key,
        "created_at": datetime.utcnow().isoformat()
    })

    _save_all_keys(keys)
    _log(f"✔ Nueva clave añadida. Total = {len(keys)}")


def _debug_watcher_api(watcher):
    """Mostrar exactamente los atributos disponibles en watcher.lol_status"""
    try:
        sub = watcher.lol_status
        _log(f"Atributos en watcher.lol_status: {dir(sub)}")
    except Exception as e:
        _log(f"ERROR inspeccionando watcher: {e}")


def get_api_key():
    """
    Selecciona y valida la primera API Key funcional.
    Usa RiotWatcher.account.by_riot_id:
    - 404 -> key valida (usuario inexistente)
    - 401 / 403 -> key invalida o caducada
    """
    _log("=== BUSCANDO API KEY VALIDA ===")

    candidates = []

    # 1) Clave del .env (prioridad)
    env_key = os.getenv("RIOT_API_KEY")
    if env_key:
        _log(f"Clave en .env detectada: ***{env_key[-6:]}")
        candidates.append(env_key)

    # 2) Claves guardadas en JSON
    saved = _load_all_keys()
    saved_keys = [k.get("key") for k in saved if k.get("key")]
    _log(f"Claves guardadas detectadas: {saved_keys}")

    for key in saved_keys:
        candidates.append(key)

    if not candidates:
        raise RuntimeError("No hay API Keys disponibles para validar.")

    _log(f"Claves a probar (en orden): {[c[-6:] for c in candidates]}")

    region = os.getenv("REGIONAL_ROUTING", "europe")
    test_name = "ApiKeyCheckUser"
    test_tag = "EUW"

    for key in candidates:
        short = key[-6:]
        _log(f"Probando API Key ***{short}")

        rw = RiotWatcher(key)

        try:
            rw.account.by_riot_id(region, test_name, test_tag)
            _log(f"API Key valida (respuesta 2xx inesperada) ***{short}")
            return key

        except ApiError as e:
            status = e.response.status_code

            if status == 404:
                _log(f"API Key valida (404 correcto) ***{short}")
                return key

            if status in (401, 403):
                _log(f"API Key invalida ***{short}: {status}")
                continue

            _log(f"Error inesperado con ***{short}: {status}")
            continue

        except Exception as e:
            _log(f"Error de red o desconocido con ***{short}: {e}")
            continue

    raise RuntimeError("Ninguna API Key funciono.")
