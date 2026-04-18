"""
Monitoring Routes — Prometheus-compatible metrics and dashboard JSON endpoint.
Exposes query volume, latency, success rates, and error breakdowns.

CONSOLIDATED: Uses the single MetricsCollector from app.observability.metrics
instead of maintaining a separate duplicate metrics system.
"""

from fastapi import APIRouter
from app.observability.metrics import metrics, QUERIES_TOTAL, QUERY_LATENCY

router = APIRouter(prefix="/api/v1/metrics", tags=["Monitoring"])


class QueryMetricsRecorder:
    """
    Thin wrapper that records query-level metrics into the unified MetricsCollector.
    This replaces the old duplicate MetricsCollector that was in this file.
    """

    def record_query(
        self,
        latency_ms: float,
        intent: str = "unknown",
        success: bool = True,
        error_agent: str = None,
    ):
        """Record a single query execution into the unified metrics system."""
        status = "success" if success else "error"
        metrics.increment(QUERIES_TOTAL, {"intent": intent, "status": status})
        metrics.observe(QUERY_LATENCY, latency_ms, {"intent": intent})

        if not success and error_agent:
            metrics.increment("plainsql_errors_by_agent", {"agent": error_agent})


# Module-level singleton
_recorder = QueryMetricsRecorder()


def get_metrics_collector() -> QueryMetricsRecorder:
    """Get the global metrics recorder singleton."""
    return _recorder


def create_monitoring_router() -> APIRouter:
    """Factory to create monitoring router."""

    @router.get("/prometheus")
    def prometheus_metrics():
        """Expose metrics in Prometheus text format for scraping."""
        from fastapi.responses import PlainTextResponse

        lines = []

        # Counters
        for key, value in metrics.counters.items():
            safe_key = _prom_safe(key)
            lines.append(f"# TYPE {safe_key} counter")
            lines.append(f"{safe_key} {value}")

        # Gauges
        for key, value in metrics.gauges.items():
            safe_key = _prom_safe(key)
            lines.append(f"# TYPE {safe_key} gauge")
            lines.append(f"{safe_key} {value}")

        # Histograms (summary stats)
        for key in set(k.split("{")[0] for k in metrics.histograms):
            stats = metrics.get_histogram_stats(key)
            if stats["count"]:
                safe_key = _prom_safe(key)
                lines.append(f"# TYPE {safe_key} summary")
                lines.append(f'{safe_key}_count {stats["count"]}')
                lines.append(f'{safe_key}_avg {stats["avg"]}')
                lines.append(f'{safe_key}{{quantile="0.5"}} {stats["p50"]}')
                lines.append(f'{safe_key}{{quantile="0.95"}} {stats["p95"]}')
                lines.append(f'{safe_key}{{quantile="0.99"}} {stats["p99"]}')

        return PlainTextResponse(
            content="\n".join(lines) + "\n",
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    @router.get("/dashboard")
    def dashboard_metrics():
        """JSON metrics for the frontend or internal dashboard."""
        return metrics.get_all_metrics()

    return router


def _prom_safe(key: str) -> str:
    """Convert a label-style key to a Prometheus-safe metric name."""
    return key.replace("{", "_").replace("}", "").replace(",", "_").replace('"', "").replace("=", "_")
