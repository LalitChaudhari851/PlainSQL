"""
API Middleware — Auth verification, rate limiting, and request logging.
"""

import time
import json
import hashlib
import structlog
from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
from typing import Optional

from app.auth.jwt_auth import AuthService
from app.observability.metrics import metrics, AUTH_FAILURES, ACTIVE_REQUESTS

logger = structlog.get_logger()


# ── In-Memory Rate Limiter (upgrade to Redis for production) ──

class InMemoryRateLimiter:
    """
    Simple in-memory rate limiter using sliding window counters.
    For production, use Redis-backed implementation.
    """

    def __init__(self, requests_per_minute: int = 60):
        self.rpm = requests_per_minute
        self._counters: dict[str, list[float]] = {}
        self._call_count = 0

    def check(self, key: str) -> bool:
        """Returns True if request is allowed, False if rate limited."""
        now = time.time()
        window_start = now - 60

        if key not in self._counters:
            self._counters[key] = []

        # Clean old entries for this key
        self._counters[key] = [t for t in self._counters[key] if t > window_start]

        # Periodic cleanup of dead keys to prevent unbounded memory growth
        self._call_count += 1
        if self._call_count % 500 == 0:
            dead_keys = [k for k, v in self._counters.items() if not v]
            for k in dead_keys:
                del self._counters[k]

        if len(self._counters[key]) >= self.rpm:
            return False

        self._counters[key].append(now)
        return True


# ── In-Memory Query Cache ────────────────────────────────

class QueryCache:
    """
    Simple in-memory query cache.
    For production, upgrade to Redis.
    """

    def __init__(self, ttl_seconds: int = 300, max_entries: int = 500):
        self.ttl = ttl_seconds
        self.max_entries = max_entries
        self._cache: dict[str, dict] = {}

    def _make_key(self, query: str, tenant_id: str = "default") -> str:
        raw = f"{tenant_id}:{query.strip().lower()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def get(self, query: str, tenant_id: str = "default") -> Optional[dict]:
        key = self._make_key(query, tenant_id)
        entry = self._cache.get(key)
        if entry and time.time() - entry["timestamp"] < self.ttl:
            return entry["data"]
        elif entry:
            del self._cache[key]
        return None

    def set(self, query: str, data: dict, tenant_id: str = "default"):
        key = self._make_key(query, tenant_id)
        # Evict oldest if at capacity
        if len(self._cache) >= self.max_entries:
            oldest_key = min(self._cache, key=lambda k: self._cache[k]["timestamp"])
            del self._cache[oldest_key]
        self._cache[key] = {"data": data, "timestamp": time.time(), "tenant_id": tenant_id}

    def invalidate(self, tenant_id: str = "default"):
        """Clear all cache entries for a specific tenant.
        Cache keys are SHA-256 hashes of '{tenant_id}:{query}', so we must
        recheck by storing the tenant_id alongside the cached data.
        """
        keys_to_delete = [
            k for k, v in self._cache.items()
            if v.get("tenant_id", "default") == tenant_id
        ]
        for k in keys_to_delete:
            del self._cache[k]

    def invalidate_all(self):
        """Clear the entire cache (all tenants)."""
        self._cache.clear()


# ── Auth Dependency ──────────────────────────────────────

def create_auth_dependency(auth_service: AuthService):
    """
    Create a FastAPI dependency for JWT authentication.
    Returns user info dict from the JWT payload.
    """
    def get_current_user(request: Request) -> dict:
        # Check for Bearer token
        auth_header = request.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        elif "X-API-Key" in request.headers:
            token = request.headers["X-API-Key"]
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing authentication token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        try:
            payload = auth_service.verify_token(token)
            return payload
        except Exception as e:
            metrics.increment(AUTH_FAILURES)
            logger.warning("auth_failed", error=str(e))
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired token",
                headers={"WWW-Authenticate": "Bearer"},
            )

    return get_current_user


# ── Request Logging Middleware ───────────────────────────

class RequestLoggingMiddleware:
    """Log all API requests with timing."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start_time = time.time()
        request = Request(scope, receive)

        # Track active requests
        metrics.set_gauge(ACTIVE_REQUESTS, metrics.gauges.get(ACTIVE_REQUESTS, 0) + 1)

        try:
            await self.app(scope, receive, send)
        finally:
            elapsed = round((time.time() - start_time) * 1000, 2)
            metrics.set_gauge(ACTIVE_REQUESTS, max(0, metrics.gauges.get(ACTIVE_REQUESTS, 0) - 1))

            logger.info(
                "http_request",
                method=request.method,
                path=request.url.path,
                elapsed_ms=elapsed,
            )
