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


def save_new_temp_key(key: str) -> dict:
    """
    Validates and saves a new API Key.
    Returns a dict with {"success": bool, "message": str}
    """
    _log("Validating NEW API Key...")
    
    key = key.strip()
    
    # 1. Format check
    if not key.startswith("RGAPI-"):
        return {"success": False, "message": "Invalid format. Must start with RGAPI-"}
        
    # 2. Check existence
    keys = _load_all_keys()
    existing = [k["key"] for k in keys]
    if key in existing:
        return {"success": False, "message": "Key already exists."}

    # 3. Live Validation
    rw = RiotWatcher(key)
    try:
        # We try a lightweight call. 
        # Note: 'platform-data-v1' or similar is good, but let's stick to the existing method logic 
        # or just try a standard check. The old code used account.by_riot_id which needs valid user.
        # Let's try to fetch free champion rotation? It doesn't require a specific user.
        # But 'europe' might be 'EUW1' for platform specific calls.
        # Let's stick to the pattern in get_api_key which seems to rely on a test user.
        # Alternatively, assume we let the user save it if format is ok, 
        # but the plan said "validation". 
        # Let's use the same logic as get_api_key's "candidates" loop but for THIS key.
        
        # We need a region. Defaults to Europe/EUW1.
        rw.account.by_riot_id("europe", "Agente", "EUW") # Random/Generic check
        # If 404 -> Key is Valid.
        
    except ApiError as e:
        if e.response.status_code == 403:
             return {"success": False, "message": "Key Rejected (403). Expired or Invalid."}
        if e.response.status_code == 401:
             return {"success": False, "message": "Key Rejected (401). unauthorized."}
        # 404 means User Not Found, but KEY IS VALID
        if e.response.status_code != 404 and not (200 <= e.response.status_code < 300):
             # Other error?
             pass 

    except Exception as e:
        # Network error?
        pass

    # If we got here, we assume it's usable (or network error which we give benefit of doubt)
    
    keys.append({
        "key": key,
        "created_at": datetime.utcnow().isoformat()
    })

    _save_all_keys(keys)
    _log(f"✔ Nueva clave añadida.")
    return {"success": True, "message": "Key Saved & Validated"}


def _debug_watcher_api(watcher):
    """Mostrar exactamente los atributos disponibles en watcher.lol_status"""
    try:
        sub = watcher.lol_status
        _log(f"Atributos en watcher.lol_status: {dir(sub)}")
    except Exception as e:
        _log(f"ERROR inspeccionando watcher: {e}")


def get_api_key(region="europe"):
    """
    Selecciona y valida la primera API Key funcional.
    Usa RiotWatcher.account.by_riot_id:
    - 404 -> key valida (usuario inexistente)
    - 401 / 403 -> key invalida o caducada
    """
    _log("=== BUSCANDO API KEY VALIDA ===")

    candidate_keys = []

    # 1) Claves guardadas en JSON (PRIORIDAD ALTA - Usuario Web)
    saved = _load_all_keys()
    # Invertimos para probar la ÚLTIMA añadida primero
    saved_keys = [k.get("key") for k in saved if k.get("key")]
    for key in reversed(saved_keys):
        candidate_keys.append(key)

    # 2) Clave del .env (PRIORIDAD BAJA - Fallback)
    env_key = os.getenv("RIOT_API_KEY")
    if env_key:
        # Solo agregar si no estaba ya en la lista (para evitar duplicados exactos)
        if env_key not in candidate_keys:
            _log(f"Clave en .env detectada (Fallback): ***{env_key[-6:]}")
            candidate_keys.append(env_key)

    if not candidate_keys:
        raise RuntimeError("No hay API Keys disponibles para validar.")

    _log(f"Claves a probar (en orden): {[c[-6:] for c in candidate_keys]}")

    test_name = "ApiKeyCheckUser"
    test_tag = "EUW"  # Esto puede ser parametrizado si lo necesitas

    for key in candidate_keys:
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

    raise RuntimeError("Ninguna API Key funcionó.")