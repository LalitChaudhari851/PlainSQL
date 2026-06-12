"""
Query Tracer — Full lifecycle tracing for every query through the pipeline.
Integrates with LangSmith (when configured), OpenTelemetry (when configured),
and local structured logging.
"""

import os
import structlog
from contextlib import contextmanager
from typing import Optional
from app.observability.metrics import metrics

logger = structlog.get_logger()


# ── OpenTelemetry Setup ─────────────────────────────────
_otel_tracer = None

def _init_otel():
    """Initialize OpenTelemetry tracer if OTLP endpoint is configured."""
    global _otel_tracer

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        return

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource

        resource = Resource.create({
            "service.name": os.getenv("OTEL_SERVICE_NAME", "plainsql"),
            "service.version": os.getenv("APP_VERSION", "2.0.0"),
        })

        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

        _otel_tracer = trace.get_tracer("plainsql.pipeline")
        logger.info("otel_tracing_initialized", endpoint=endpoint)

    except ImportError:
        logger.info("otel_not_installed",
                     hint="pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc")
    except Exception as e:
        logger.warning("otel_init_failed", error=str(e))


# Initialize on module load
_init_otel()


@contextmanager
def trace_span(name: str, attributes: dict = None):
    """
    Context manager for creating trace spans.

    Usage:
        with trace_span("sql_generation", {"query": user_query}):
            result = generate_sql(...)

    When OpenTelemetry is configured, creates real spans exported via OTLP.
    When not configured, acts as a no-op (zero overhead).
    """
    if _otel_tracer:
        with _otel_tracer.start_as_current_span(name) as span:
            if attributes:
                for k, v in attributes.items():
                    span.set_attribute(k, str(v) if not isinstance(v, (int, float, bool)) else v)
            try:
                yield span
            except Exception as e:
                span.set_attribute("error", True)
                span.set_attribute("error.message", str(e)[:200])
                raise
    else:
        yield None


class QueryTracer:
    """
    Traces the full lifecycle of a user query through the agent pipeline.
    Logs to structlog, optionally to LangSmith, and optionally to OpenTelemetry.
    """

    def __init__(self, langsmith_api_key: Optional[str] = None, project: str = "plainsql"):
        self.langsmith_enabled = False
        self.langsmith_client = None

        if langsmith_api_key:
            try:
                from langsmith import Client
                self.langsmith_client = Client(api_key=langsmith_api_key)
                self.langsmith_enabled = True
                logger.info("langsmith_connected", project=project)
            except ImportError:
                logger.warning("langsmith_not_installed")
            except Exception as e:
                logger.warning("langsmith_init_failed", error=str(e))

    def trace_query(self, state: dict):
        """Log the complete query lifecycle."""
        trace_id = state.get("trace_id", "unknown")
        intent = state.get("intent", "unknown")
        has_error = bool(state.get("error"))

        # ── Metrics ──────────────────────────────────────
        status = "error" if has_error else "success"
        metrics.increment("plainsql_queries_total", {"intent": intent, "status": status})

        execution_time = state.get("execution_time_ms", 0)
        if execution_time:
            metrics.observe("plainsql_query_latency_ms", execution_time, {"intent": intent})

        # ── Structured Log ───────────────────────────────
        log_data = {
            "trace_id": trace_id,
            "user_query": state.get("user_query", ""),
            "intent": intent,
            "complexity": state.get("complexity", "unknown"),
            "generated_sql": state.get("generated_sql", "")[:200],
            "is_valid": state.get("is_valid"),
            "execution_time_ms": execution_time,
            "row_count": state.get("row_count", 0),
            "retry_count": state.get("retry_count", 0),
            "error": state.get("error"),
            "error_agent": state.get("error_agent"),
            "tenant_id": state.get("tenant_id", "default"),
            "prompt_version": state.get("prompt_version", "unknown"),
        }

        if has_error:
            logger.error("query_lifecycle", **log_data)
        else:
            logger.info("query_lifecycle", **log_data)

        # ── OpenTelemetry Span ───────────────────────────
        if _otel_tracer:
            with _otel_tracer.start_as_current_span("query_lifecycle") as span:
                for k, v in log_data.items():
                    if v is not None:
                        span.set_attribute(k, str(v) if not isinstance(v, (int, float, bool)) else v)

        # ── LangSmith Trace ──────────────────────────────
        if self.langsmith_enabled and self.langsmith_client:
            try:
                self.langsmith_client.create_run(
                    name="plainsql_query",
                    run_type="chain",
                    inputs={"query": state.get("user_query", "")},
                    outputs={
                        "sql": state.get("generated_sql", ""),
                        "row_count": state.get("row_count", 0),
                        "intent": intent,
                    },
                    error=state.get("error"),
                    extra={"metadata": log_data},
                )
            except Exception as e:
                logger.warning("langsmith_trace_failed", error=str(e))

    def get_dashboard_metrics(self) -> dict:
        """Get metrics for the monitoring dashboard."""
        return {
            "total_queries": metrics.get_counter("plainsql_queries_total"),
            "successful_queries": metrics.get_counter("plainsql_queries_total", {"status": "success"}),
            "failed_queries": metrics.get_counter("plainsql_queries_total", {"status": "error"}),
            "latency_stats": metrics.get_histogram_stats("plainsql_query_latency_ms"),
            "all_metrics": metrics.get_all_metrics(),
        }

