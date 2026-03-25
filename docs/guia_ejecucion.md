# Guia de Ejecucion

Instrucciones para arrancar el proyecto.

## Requisitos
Levantar las bases de datos con Docker:
```bash
docker-compose up -d
```

## Ejecucion del Dashboard
Desde la raiz del proyecto:
```bash
streamlit run dashboard/app.py
```

En Windows con venv:
```powershell
.\.venv\Scripts\streamlit run dashboard/app.py
```

La aplicacion principal esta en `dashboard/app.py` y las paginas en `dashboard/pages/`.
