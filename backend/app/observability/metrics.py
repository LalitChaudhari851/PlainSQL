"""
Prometheus Metrics — Production-grade application metrics.

Uses prometheus_client for native Prometheus exposition format.
Falls back to a lightweight in-process collector if prometheus_client
is not installed (e.g., minimal local dev).

Exposes /metrics endpoint for Prometheus scraping.
"""

import time
from functools import wraps

try:
    from prometheus_client import (
        Counter, Histogram, Gauge, Info,
        generate_latest, CONTENT_TYPE_LATEST,
        CollectorRegistry, REGISTRY,
    )
    _PROM_AVAILABLE = True
except ImportError:
    _PROM_AVAILABLE = False


if _PROM_AVAILABLE:
    # ── Prometheus Metrics (production) ──────────────────────

    # Counters
    QUERIES_TOTAL = Counter(
        "plainsql_queries_total",
        "Total queries processed",
        ["intent", "status"],
    )
    LLM_CALLS_TOTAL = Counter(
        "plainsql_llm_calls_total",
        "Total LLM API calls",
        ["provider", "status"],
    )
    CACHE_OPS_TOTAL = Counter(
        "plainsql_cache_ops_total",
        "Cache operations",
        ["op"],  # hit, miss, write
    )
    VALIDATION_FAILURES_TOTAL = Counter(
        "plainsql_validation_failures_total",
        "SQL validation failures",
    )
    AUTH_FAILURES_TOTAL = Counter(
        "plainsql_auth_failures_total",
        "Authentication failures",
    )
    AGENT_ERRORS_TOTAL = Counter(
        "plainsql_agent_errors_total",
        "Agent-level errors",
        ["agent"],
    )

    # Histograms
    QUERY_LATENCY = Histogram(
        "plainsql_query_latency_ms",
        "End-to-end query latency in milliseconds",
        ["intent"],
        buckets=[50, 100, 250, 500, 1000, 2000, 5000, 10000, 30000, 60000],
    )
    AGENT_LATENCY = Histogram(
        "plainsql_agent_latency_ms",
        "Per-agent latency in milliseconds",
        ["agent"],
        buckets=[10, 50, 100, 250, 500, 1000, 5000, 10000],
    )
    LLM_LATENCY = Histogram(
        "plainsql_llm_latency_ms",
        "LLM API call latency in milliseconds",
        ["provider"],
        buckets=[100, 500, 1000, 2000, 5000, 10000, 30000],
    )

    # Gauges
    ACTIVE_REQUESTS = Gauge(
        "plainsql_active_requests",
        "Number of in-flight requests",
    )


    class MetricsCollector:
        """
        Prometheus-backed metrics collector.
        Wraps prometheus_client objects with a consistent API.
        """

        def __init__(self):
            pass  # Metrics are module-level singletons

        def increment(self, name: str, labels: dict = None, value: int = 1):
            """Increment a counter metric."""
            if name == "plainsql_queries_total":
                QUERIES_TOTAL.labels(**(labels or {})).inc(value)
            elif name == "plainsql_llm_calls_total":
                LLM_CALLS_TOTAL.labels(**(labels or {})).inc(value)
            elif name == "plainsql_cache_ops_total":
                CACHE_OPS_TOTAL.labels(**(labels or {})).inc(value)
            elif name == "plainsql_validation_failures_total":
                VALIDATION_FAILURES_TOTAL.inc(value)
            elif name == "plainsql_auth_failures_total":
                AUTH_FAILURES_TOTAL.inc(value)
            elif name == "plainsql_agent_errors_total":
                AGENT_ERRORS_TOTAL.labels(**(labels or {})).inc(value)

        def observe(self, name: str, value: float, labels: dict = None):
            """Record a value in a histogram."""
            if name == "plainsql_query_latency_ms":
                QUERY_LATENCY.labels(**(labels or {})).observe(value)
            elif name == "plainsql_agent_latency_ms":
                AGENT_LATENCY.labels(**(labels or {})).observe(value)
            elif name == "plainsql_llm_latency_ms":
                LLM_LATENCY.labels(**(labels or {})).observe(value)

        def set_gauge(self, name: str, value: float, labels: dict = None):
            """Set a gauge to a specific value."""
            if name == "plainsql_active_requests":
                ACTIVE_REQUESTS.set(value)

        def get_counter(self, name: str, labels: dict = None) -> int:
            """Get current counter value (approximate — Prometheus counters are append-only)."""
            try:
                if name == "plainsql_queries_total" and labels:
                    return int(QUERIES_TOTAL.labels(**labels)._value.get())
                elif name == "plainsql_queries_total":
                    # Sum across all label combinations
                    return int(sum(
                        sample.value for sample in QUERIES_TOTAL.collect()[0].samples
                        if sample.name == "plainsql_queries_total_total"
                    ))
            except Exception:
                pass
            return 0

        def get_histogram_stats(self, name: str, labels: dict = None) -> dict:
            """Get histogram statistics. For Prometheus, stats are computed server-side."""
            return {"count": 0, "avg": 0, "p50": 0, "p95": 0, "p99": 0, "min": 0, "max": 0}

        def get_all_metrics(self) -> dict:
            """Return Prometheus metrics text for debugging."""
            return {"format": "prometheus", "endpoint": "/metrics"}

        @staticmethod
        def _make_key(name: str, labels: dict = None) -> str:
            if not labels:
                return name
            label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
            return f"{name}{{{label_str}}}"

    def get_prometheus_metrics() -> bytes:
        """Generate Prometheus exposition format output."""
        return generate_latest(REGISTRY)

    def get_prometheus_content_type() -> str:
        """Get the correct Content-Type for Prometheus metrics."""
        return CONTENT_TYPE_LATEST

else:
    # ── Fallback: In-Process Metrics (development) ───────────

    class MetricsCollector:
        """
        In-process metrics collector (no prometheus_client dependency).
        Tracks counters, histograms, and gauges for monitoring.
        """

        def __init__(self):
            self.counters: dict[str, int] = {}
            self.histograms: dict[str, list[float]] = {}
            self.gauges: dict[str, float] = {}

        _HISTOGRAM_CAP = 10_000

        def increment(self, name: str, labels: dict = None, value: int = 1):
            key = self._make_key(name, labels)
            self.counters[key] = self.counters.get(key, 0) + value

        def observe(self, name: str, value: float, labels: dict = None):
            key = self._make_key(name, labels)
            if key not in self.histograms:
                self.histograms[key] = []
            self.histograms[key].append(value)
            if len(self.histograms[key]) > self._HISTOGRAM_CAP:
                self.histograms[key] = self.histograms[key][-self._HISTOGRAM_CAP // 2:]

        def set_gauge(self, name: str, value: float, labels: dict = None):
            key = self._make_key(name, labels)
            self.gauges[key] = value

        def get_counter(self, name: str, labels: dict = None) -> int:
            key = self._make_key(name, labels)
            return self.counters.get(key, 0)

        def get_histogram_stats(self, name: str, labels: dict = None) -> dict:
            key = self._make_key(name, labels)
            values = self.histograms.get(key, [])
            if not values:
                return {"count": 0, "avg": 0, "p50": 0, "p95": 0, "p99": 0, "min": 0, "max": 0}
            sorted_vals = sorted(values)
            n = len(sorted_vals)
            return {
                "count": n,
                "avg": round(sum(sorted_vals) / n, 2),
                "p50": round(sorted_vals[int(n * 0.5)], 2),
                "p95": round(sorted_vals[int(n * 0.95)], 2) if n > 20 else round(sorted_vals[-1], 2),
                "p99": round(sorted_vals[int(n * 0.99)], 2) if n > 100 else round(sorted_vals[-1], 2),
                "min": round(sorted_vals[0], 2),
                "max": round(sorted_vals[-1], 2),
            }

        def get_all_metrics(self) -> dict:
            return {
                "counters": dict(self.counters),
                "histograms": {
                    k: self.get_histogram_stats(k) for k in set(
                        k.split("{")[0] for k in self.histograms
                    )
                },
                "gauges": dict(self.gauges),
            }

        @staticmethod
        def _make_key(name: str, labels: dict = None) -> str:
            if not labels:
                return name
            label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
            return f"{name}{{{label_str}}}"

    def get_prometheus_metrics() -> bytes:
        """Fallback: return empty metrics."""
        return b""

    def get_prometheus_content_type() -> str:
        return "text/plain"


# ── Singleton Metrics Instance ───────────────────────────
metrics = MetricsCollector()

# ── Pre-defined metric name constants (backward compat) ──
QUERIES_TOTAL_NAME = "plainsql_queries_total"
QUERY_LATENCY_NAME = "plainsql_query_latency_ms"
LLM_CALLS_NAME = "plainsql_llm_calls_total"
LLM_LATENCY_NAME = "plainsql_llm_latency_ms"
LLM_TOKENS_NAME = "plainsql_llm_tokens_total"
CACHE_HITS_NAME = "plainsql_cache_hits_total"
CACHE_MISSES_NAME = "plainsql_cache_misses_total"
VALIDATION_FAILURES_NAME = "plainsql_validation_failures_total"
AUTH_FAILURES_NAME = "plainsql_auth_failures_total"
ACTIVE_REQUESTS_NAME = "plainsql_active_requests"

# Keep backward-compatible aliases
QUERIES_TOTAL_KEY = QUERIES_TOTAL_NAME
QUERY_LATENCY_KEY = QUERY_LATENCY_NAME
LLM_CALLS_TOTAL_KEY = LLM_CALLS_NAME
LLM_LATENCY_KEY = LLM_LATENCY_NAME
LLM_TOKENS = LLM_TOKENS_NAME
CACHE_HITS = CACHE_HITS_NAME
CACHE_MISSES = CACHE_MISSES_NAME
VALIDATION_FAILURES = VALIDATION_FAILURES_NAME
AUTH_FAILURES = AUTH_FAILURES_NAME
ACTIVE_REQUESTS_KEY = ACTIVE_REQUESTS_NAME
