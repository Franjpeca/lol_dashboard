# LoL Data Dashboard

Dashboard interactivo de datos de partidas del videojuego League of Legends.
Permite extraer partidas dada la API de Riot Games y procesarlas para obtener estadisticas y visualizaciones.

## Características

- **Ingesta de Datos**: Extracción automática de partidas desde la API de Riot Games.
- **Pipeline ETL**: Procesamiento de datos crudos (MongoDB) a un formato analítico estructurado (PostgreSQL).
- **Dashboard Interactivo**: Visualización de winrate, estadísticas por jugador, minería de datos de composiciones y más.
- **Fácil despliegue**: Listo para desplegar con Docker Compose.

## Estructura del Proyecto

- `dashboard/`: Aplicación web y páginas de visualización.
- `src/`: Lógica central del pipeline y utilidades.
- `scripts/`: Scripts operativos y de gestión de base de datos.
- `docs/`: Guías detalladas de comandos y ejecución.
- `data/`: Almacenamiento local de configuraciones, partiadas y resultados temporales.

## Requisitos Previos

- Python 3.10+
- Docker y Docker Compose
- Riot Games API Key

## Instalación Rápida

1. **Clonar el repositorio** e instalar dependencias:
   ```bash
   pip install -r requirements.txt
   ```

2. **Configurar el entorno**:
   Copia el archivo de ejemplo y añade tus credenciales:
   ```bash
   cp .env.example .env
   ```

3. **Levantar servicios**:
   ```bash
   docker-compose up -d
   ```

4. **Ejecutar el Dashboard**:
   ```bash
   streamlit run dashboard/app.py
   ```

## Documentación

Para más información, consulta los documentos en la carpeta `docs/`:
- [Guía de Comandos](docs/comandos.md)
- [Guía de Ejecución](docs/guia_ejecucion.md)
