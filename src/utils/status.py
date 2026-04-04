import json
import datetime
from pathlib import Path

def save_last_update():
    """Guarda la fecha y hora de la última actualización exitosa en data/last_update.json."""
    # Encontrar la raíz del proyecto (basado en que este archivo está en src/utils/)
    root_dir = Path(__file__).resolve().parents[2]
    path = root_dir / "data" / "last_update.json"
    
    # Crear carpeta data si no existe
    path.parent.mkdir(parents=True, exist_ok=True)
    
    now = datetime.datetime.now()
    data = {
        "last_update": now.strftime("%d/%m/%Y %H:%M:%S")
    }
    
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4)
    print(f"[STATUS] 📅 Última actualización registrada: {data['last_update']}")

def get_last_update_str():
    """Lee la fecha de la última actualización."""
    root_dir = Path(__file__).resolve().parents[2]
    path = root_dir / "data" / "last_update.json"
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f).get("last_update", "Desconocida")
        except:
            return "Error lectura"
    return "N/A"
