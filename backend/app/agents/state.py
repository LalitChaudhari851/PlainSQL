"""
Agent State — Shared TypedDict for the LangGraph multi-agent pipeline.
Every agent reads from and writes to this state object.
"""

from typing import TypedDict, Optional, Literal


class AgentState(TypedDict, total=False):
    """
    Shared state flowing through the LangGraph agent pipeline.
    Each agent enriches specific fields and passes state forward.
    """

    # ── Input ────────────────────────────────────────────
    user_query: str
    conversation_history: list[dict]
    tenant_id: str
    user_role: str
    trace_id: str

    # ── Query Understanding Agent Output ─────────────────
    intent: Literal[
        "chat",             # Natural conversation
        "sql",              # Database query that should continue to SQL generation
    ]
    route_intent: Literal[
        "chat",             # Hi, hello, thanks, capabilities, etc.
        "data_query",       # SELECT with filters
        "aggregation",      # COUNT, SUM, AVG, GROUP BY
        "comparison",       # Compare datasets
        "explanation",      # Explain a concept or previous query
        "meta_query",       # Questions about the schema itself
    ]
    entities: list[str]                # Extracted table/column names
    complexity: Literal["simple", "moderate", "complex"]

    # ── Schema Retrieval Agent Output ────────────────────
    relevant_schema: str               # Formatted schema context for LLM
    relevant_tables: list[str]         # Table names retrieved

    # ── SQL Generation Agent Output ──────────────────────
    generated_sql: str                 # The SQL query
    sql_explanation: str               # Technical explanation
    friendly_message: str              # User-friendly message

    # ── SQL Validation Agent Output ──────────────────────
    is_valid: bool                     # Safety check passed
    validation_errors: list[str]       # List of issues found
    sanitized_sql: str                 # SQL after safety modifications (LIMIT injection, etc.)
    retry_count: int                   # Number of regeneration attempts

    # ── Output Guardrail Results ─────────────────────────
    guardrail_warnings: list[str]      # Hallucinated reference warnings
    guardrail_confidence: float        # LLM output confidence score (0.0-1.0)

    # ── Execution Agent Output ───────────────────────────
    query_results: list[dict]          # Raw query results
    execution_time_ms: float           # Query execution duration
    row_count: int                     # Number of rows returned
    column_names: list[str]            # Column names in result set

    # ── Visualization Agent Output ───────────────────────
    chart_config: Optional[dict]       # Chart.js configuration
    chart_type: Optional[str]          # bar, line, pie, doughnut, scatter
    insights: list[str]                # Auto-generated insights
    follow_up_questions: list[str]     # Suggested follow-up queries

    # ── Error Handling ───────────────────────────────────
    error: Optional[str]               # Error message if any step failed
    error_agent: Optional[str]         # Which agent produced the error
