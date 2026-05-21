#!/bin/sh
set -e

echo "=== PlainSQL Production Startup ==="

# Run database migrations
echo "Running Alembic migrations..."
cd /app
alembic upgrade head || echo "WARNING: Migrations failed — check database connectivity"

# Start Gunicorn
echo "Starting Gunicorn with Uvicorn workers..."
exec gunicorn app.main:app \
     --worker-class uvicorn.workers.UvicornWorker \
     --workers ${GUNICORN_WORKERS:-4} \
     --bind 0.0.0.0:8000 \
     --timeout 120 \
     --graceful-timeout 30 \
     --keep-alive 5 \
     --access-logfile - \
     --error-logfile -
