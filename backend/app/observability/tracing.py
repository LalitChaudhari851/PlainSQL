"""
Query Tracer — Full lifecycle tracing for every query through the pipeline.
Integrates with LangSmith (when configured) and local structured logging.
"""

import time
import structlog
from typing import Optional
from app.observability.metrics import metrics, QUERIES_TOTAL, QUERY_LATENCY

logger = structlog.get_logger()


class QueryTracer:
    """
    Traces the full lifecycle of a user query through the agent pipeline.
    Logs to structlog and optionally to LangSmith for LLM observability.
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
        metrics.increment(QUERIES_TOTAL, {"intent": intent, "status": status})

        execution_time = state.get("execution_time_ms", 0)
        if execution_time:
            metrics.observe(QUERY_LATENCY, execution_time, {"intent": intent})

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
        }

        if has_error:
            logger.error("query_lifecycle", **log_data)
        else:
            logger.info("query_lifecycle", **log_data)

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
            "total_queries": metrics.get_counter(QUERIES_TOTAL),
            "successful_queries": metrics.get_counter(QUERIES_TOTAL, {"status": "success"}),
            "failed_queries": metrics.get_counter(QUERIES_TOTAL, {"status": "error"}),
            "latency_stats": metrics.get_histogram_stats(QUERY_LATENCY),
            "all_metrics": metrics.get_all_metrics(),
        }
