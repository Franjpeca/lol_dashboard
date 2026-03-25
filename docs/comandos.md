# Guia de Comandos

Guia de comandos para operar el pipeline de datos y el dashboard.

## 0. Entorno
Asegurate de estar en la raiz y activar el venv:
```bash
source .venv/bin/activate
```

## 1. Descarga (L0)
Descarga partidas nuevas de la API de Riot:
```bash
python src/pipeline.py --mode l0 --run-in-terminal
```

## 2. ETL (Pipeline)
Procesa los datos para el Dashboard.

### Orquestador Completo (Recomendado)
Actualiza todos los filtros automaticamente:
```bash
python scripts/run_pipeline_all.py
```

### Manual (Por Pool)
Actualizacion especifica:
```bash
python src/pipeline.py --mode l1-l2 --min 1 --run-in-terminal  # Normal
python src/pipeline.py --mode season --min 2 --run-in-terminal # Season
```

## 3. Dashboard
Lanza el servidor web:
```bash
streamlit run dashboard/app.py --server.port 8080
```
