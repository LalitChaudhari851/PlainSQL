"""
Prometheus Metrics — Application-level metrics for monitoring dashboards.
Exposes /metrics endpoint for Prometheus scraping.
"""

import time
from functools import wraps


class MetricsCollector:
    """
    In-process metrics collector.
    Tracks counters, histograms, and gauges for monitoring.
    Uses a simple dict-based approach for portability (no Prometheus dependency required).
    Can be upgraded to prometheus_client if Prometheus is deployed.
    """

    def __init__(self):
        self.counters: dict[str, int] = {}
        self.histograms: dict[str, list[float]] = {}
        self.gauges: dict[str, float] = {}

    # ── Counters ─────────────────────────────────────────

    def increment(self, name: str, labels: dict = None, value: int = 1):
        """Increment a counter metric."""
        key = self._make_key(name, labels)
        self.counters[key] = self.counters.get(key, 0) + value

    # ── Histograms ───────────────────────────────────────

    def observe(self, name: str, value: float, labels: dict = None):
        """Record a value in a histogram."""
        key = self._make_key(name, labels)
        if key not in self.histograms:
            self.histograms[key] = []
        self.histograms[key].append(value)

    # ── Gauges ───────────────────────────────────────────

    def set_gauge(self, name: str, value: float, labels: dict = None):
        """Set a gauge to a specific value."""
        key = self._make_key(name, labels)
        self.gauges[key] = value

    # ── Query Methods ────────────────────────────────────

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
        """Get all metrics as a structured dict."""
        return {
            "counters": dict(self.counters),
            "histograms": {
                k: self.get_histogram_stats(k) for k in set(
                    k.split("{")[0] for k in self.histograms
                )
            },
            "gauges": dict(self.gauges),
        }

    # ── Helpers ───────────────────────────────────────────

    @staticmethod
    def _make_key(name: str, labels: dict = None) -> str:
        if not labels:
            return name
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"


# ── Singleton Metrics Instance ───────────────────────────
metrics = MetricsCollector()

# ── Pre-defined metric names ─────────────────────────────
QUERIES_TOTAL = "plainsql_queries_total"
QUERY_LATENCY = "plainsql_query_latency_ms"
LLM_CALLS_TOTAL = "plainsql_llm_calls_total"
LLM_LATENCY = "plainsql_llm_latency_ms"
LLM_TOKENS = "plainsql_llm_tokens_total"
CACHE_HITS = "plainsql_cache_hits_total"
CACHE_MISSES = "plainsql_cache_misses_total"
VALIDATION_FAILURES = "plainsql_validation_failures_total"
AUTH_FAILURES = "plainsql_auth_failures_total"
ACTIVE_REQUESTS = "plainsql_active_requests"
