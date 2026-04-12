"""
Execution Agent — Safely executes validated SQL against the tenant database.
Tracks execution time and handles errors gracefully.
"""

import time
import structlog

from app.agents.state import AgentState

logger = structlog.get_logger()


def execution_node(state: AgentState, db_pool) -> dict:
    """
    Execute the validated SQL query against the database.
    Measures execution time and returns structured results.
    """
    sql = state.get("sanitized_sql", "") or state.get("generated_sql", "")
    trace_id = state.get("trace_id", "unknown")

    logger.info("agent_started", agent="execution", trace_id=trace_id)

    if not sql or not sql.strip():
        return {
            "query_results": [],
            "execution_time_ms": 0,
            "row_count": 0,
            "column_names": [],
            "error": "No SQL to execute",
            "error_agent": "execution",
        }

    try:
        start_time = time.perf_counter()
        results = db_pool.execute_query(sql)
        end_time = time.perf_counter()

        execution_time_ms = round((end_time - start_time) * 1000, 2)

        # Extract column names from results
        column_names = list(results[0].keys()) if results else []

        logger.info(
            "query_executed",
            execution_time_ms=execution_time_ms,
            row_count=len(results),
            columns=len(column_names),
        )

        return {
            "query_results": results,
            "execution_time_ms": execution_time_ms,
            "row_count": len(results),
            "column_names": column_names,
        }

    except Exception as e:
        error_msg = str(e)
        logger.error("query_execution_failed", error=error_msg, sql_preview=sql[:100])

        return {
            "query_results": [],
            "execution_time_ms": 0,
            "row_count": 0,
            "column_names": [],
            "error": f"Database error: {error_msg}",
            "error_agent": "execution",
            "friendly_message": f"The query had a syntax error: {error_msg}",
        }
