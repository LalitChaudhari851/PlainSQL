# ── Docker Multi-Stage Build ─────────────────────────────
# Stage 1: Dependencies
FROM python:3.11-slim AS deps

WORKDIR /app
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Stage 2: Application
FROM python:3.11-slim AS runtime

WORKDIR /app

# Copy installed packages from deps stage
COPY --from=deps /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=deps /usr/local/bin /usr/local/bin

# Copy application code
COPY backend/ .
COPY frontend/ /frontend/
COPY .env.example .env.example

# Create non-root user
RUN useradd -m -r plainsql && \
    chown -R plainsql:plainsql /app && \
    mkdir -p /app/chroma_db && \
    chown -R plainsql:plainsql /app/chroma_db

USER plainsql

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/api/v1/health')" || exit 1

EXPOSE 8000

# Single worker avoids concurrent ChromaDB migrations in the local container volume.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--access-log"]
