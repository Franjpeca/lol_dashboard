# Guía de Ejecución - Dashboard LoL

Este dashboard está construido con **Streamlit** y requiere de MongoDB y PostgreSQL para funcionar correctamente.

## Requisitos Previos

1. **Bases de Datos**: Asegúrate de que los contenedores de Docker estén corriendo.
   ```powershell
   docker-compose up -d
   ```

2. **Entorno Python**: Se recomienda usar el entorno virtual `.venv` ya configurado.

## Comandos de Ejecución

### Desde PowerShell (Recomendado)
Para lanzar el servidor directamente usando el entorno virtual:
```powershell
.\.venv\Scripts\streamlit run dashboard/app.py
```

### Alternativa con Python
Si tienes `streamlit` instalado globalmente o en tu entorno activo:
```powershell
streamlit run dashboard/app.py
```

## Estructura del Dashboard
El punto de entrada principal es `dashboard/app.py`. Las diferentes pestañas y secciones se encuentran en el directorio `dashboard/pages/`.
