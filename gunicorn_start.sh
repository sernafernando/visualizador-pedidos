#!/bin/bash

# Ruta al directorio raíz de tu proyecto
PROJECT_ROOT="/var/www/html/visualizador-pedidos"

# Ruta al entorno virtual
VENV_PATH="$PROJECT_ROOT/backend/venv"

# Activar el entorno virtual
source "$VENV_PATH/bin/activate"

# Exportar FLASK_APP
export FLASK_APP=backend.app

# Iniciar Gunicorn
# -w: número de workers (se recomienda 2*CPU + 1)
# -b: dirección y puerto de escucha
# backend.app: el módulo de tu aplicación Flask (backend es la carpeta, app es el archivo app.py)
exec gunicorn --workers 3 --bind 0.0.0.0:8000 "backend.app:app"

# Desactivar el entorno virtual (no se ejecutará si se usa exec, pero es buena práctica)
deactivate
