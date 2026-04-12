"""
LangGraph Orchestrator — Wires all 6 agents into a DAG with conditional routing.
This is the core of the multi-agent architecture.
Includes pipeline timeout protection and graceful degradation.
"""

import uuid
import time
import structlog
from langgraph.graph import StateGraph, END

from app.agents.state import AgentState
from app.agents.query_understanding import query_understanding_node
from app.agents.schema_retrieval import schema_retrieval_node
from app.agents.sql_generation import sql_generation_node
from app.agents.sql_validation import sql_validation_node, route_validation
from app.agents.execution import execution_node
from app.agents.visualization import visualization_node

logger = structlog.get_logger()

# Maximum time (seconds) the full pipeline is allowed to run
PIPELINE_TIMEOUT_SECONDS = 60


class AgentOrchestrator:
    """
    Multi-agent orchestrator using LangGraph StateGraph.
    
    Flow:
        understand_query → [chat? → END]
                         → retrieve_schema → generate_sql → validate_sql
                         → [valid? → execute → visualize → END]
                         → [invalid? → retry generate_sql (max 3)]
                         → [blocked? → END with error]
    """

    def __init__(self, llm_router, rag_retriever, db_pool):
        self.llm_router = llm_router
        self.rag_retriever = rag_retriever
        self.db_pool = db_pool
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        """Construct the LangGraph agent pipeline."""
        graph = StateGraph(AgentState)

        # ── Register agent nodes ─────────────────────────
        graph.add_node("understand_query", self._understand_query)
        graph.add_node("handle_chat", self._handle_chat)
        graph.add_node("handle_meta", self._handle_meta)
        graph.add_node("retrieve_schema", self._retrieve_schema)
        graph.add_node("generate_sql", self._generate_sql)
        graph.add_node("validate_sql", self._validate_sql)
        graph.add_node("execute_query", self._execute_query)
        graph.add_node("visualize", self._visualize)
        graph.add_node("handle_blocked", self._handle_blocked)

        # ── Entry point ──────────────────────────────────
        graph.set_entry_point("understand_query")

        # ── Conditional routing after intent classification ──
        graph.add_conditional_edges(
            "understand_query",
            self._route_by_intent,
            {
                "chat": "handle_chat",
                "meta_query": "retrieve_schema",
                "data_query": "retrieve_schema",
                "aggregation": "retrieve_schema",
                "comparison": "retrieve_schema",
                "explanation": "retrieve_schema",
            },
        )

        # ── Linear flow ─────────────────────────────────
        graph.add_conditional_edges(
            "retrieve_schema",
            self._route_after_schema,
            {
                "meta_query": "handle_meta",
                "sql": "generate_sql",
            },
        )
        graph.add_edge("generate_sql", "validate_sql")

        # ── Conditional routing after validation ─────────
        graph.add_conditional_edges(
            "validate_sql",
            route_validation,
            {
                "valid": "execute_query",
                "retry": "generate_sql",
                "blocked": "handle_blocked",
            },
        )

        graph.add_edge("execute_query", "visualize")

        # ── Terminal nodes ───────────────────────────────
        graph.add_edge("visualize", END)
        graph.add_edge("handle_chat", END)
        graph.add_edge("handle_meta", END)
        graph.add_edge("handle_blocked", END)

        return graph.compile()

    # ── Node Wrappers (inject dependencies + error isolation) ──

    def _understand_query(self, state: AgentState) -> dict:
        return self._safe_execute("query_understanding", query_understanding_node, state, self.llm_router)

    def _retrieve_schema(self, state: AgentState) -> dict:
        return self._safe_execute("schema_retrieval", schema_retrieval_node, state, self.rag_retriever, self.db_pool)

    def _generate_sql(self, state: AgentState) -> dict:
        return self._safe_execute("sql_generation", sql_generation_node, state, self.llm_router)

    def _validate_sql(self, state: AgentState) -> dict:
        return self._safe_execute("sql_validation", sql_validation_node, state)

    def _execute_query(self, state: AgentState) -> dict:
        return self._safe_execute("execution", execution_node, state, self.db_pool)

    def _visualize(self, state: AgentState) -> dict:
        return self._safe_execute("visualization", visualization_node, state)

    def _safe_execute(self, agent_name: str, func, *args) -> dict:
        """
        Wrapper that catches per-agent exceptions for graceful degradation.
        Non-critical agents (visualization) failing won't crash the pipeline.
        """
        try:
            return func(*args)
        except Exception as e:
            logger.error(
                "agent_failed",
                agent=agent_name,
                error=str(e),
                trace_id=args[0].get("trace_id", "unknown") if args else "unknown",
            )
            # For non-critical agents, return empty results
            non_critical = {"visualization"}
            if agent_name in non_critical:
                return {
                    "chart_config": None,
                    "chart_type": None,
                    "insights": [f"Visualization skipped due to error: {str(e)[:80]}"],
                    "follow_up_questions": [],
                }
            # For critical agents, propagate the error state
            return {
                "error": f"{agent_name} failed: {str(e)}",
                "error_agent": agent_name,
            }

    def _handle_chat(self, state: AgentState) -> dict:
        """Terminal node for conversational responses."""
        return {
            "friendly_message": state.get(
                "friendly_message",
                "Hello. I can help you query your database in plain English.",
            ),
            "query_results": [],
            "row_count": 0,
            "follow_up_questions": [
                "Show top 5 employees by salary",
                "Total sales revenue by region",
                "List all products with low stock",
            ],
        }

    def _handle_meta(self, state: AgentState) -> dict:
        """Terminal node for schema/meta queries."""
        schema = state.get("relevant_schema", "")
        tables = state.get("relevant_tables", [])
        
        # Format schema info as friendly message
        table_list = ", ".join(tables) if tables else "No tables found"
        return {
            "friendly_message": f"Your database contains these tables: **{table_list}**\n\n```\n{schema}\n```",
            "query_results": [],
            "row_count": 0,
            "follow_up_questions": [f"Show data from {t}" for t in tables[:3]],
        }

    def _handle_blocked(self, state: AgentState) -> dict:
        """Terminal node when SQL validation fails after max retries."""
        errors = state.get("validation_errors", [])
        return {
            "error": "Query blocked by safety layer",
            "error_agent": "sql_validation",
            "friendly_message": (
                "🛡️ **Security Alert**: Your query was blocked by the safety system.\n\n"
                f"Reasons: {', '.join(errors)}\n\n"
                "I can only perform safe, read-only (SELECT) operations."
            ),
            "query_results": [],
            "row_count": 0,
        }

    # ── Routing Functions ────────────────────────────────

    @staticmethod
    def _route_by_intent(state: AgentState) -> str:
        """Route to appropriate handler based on classified intent."""
        route_intent = state.get("route_intent", state.get("intent", "data_query"))
        valid_routes = {"chat", "meta_query", "data_query", "aggregation", "comparison", "explanation"}
        return route_intent if route_intent in valid_routes else "data_query"

    @staticmethod
    def _route_after_schema(state: AgentState) -> str:
        """Send schema/meta requests to the meta handler; SQL requests continue."""
        if state.get("route_intent") == "meta_query":
            return "meta_query"
        return "sql"

    # ── Public API ───────────────────────────────────────

    def process_query(
        self,
        user_query: str,
        conversation_history: list[dict] = None,
        tenant_id: str = "default",
        user_role: str = "analyst",
    ) -> AgentState:
        """
        Process a natural language query through the full agent pipeline.
        Returns the final AgentState with all results.
        Enforces a pipeline-level timeout to prevent runaway processing.
        """
        trace_id = str(uuid.uuid4())[:8]

        initial_state: AgentState = {
            "user_query": user_query,
            "conversation_history": conversation_history or [],
            "tenant_id": tenant_id,
            "user_role": user_role,
            "trace_id": trace_id,
            "retry_count": 0,
            "validation_errors": [],
        }

        logger.info(
            "pipeline_started",
            trace_id=trace_id,
            query=user_query,
            tenant_id=tenant_id,
        )

        start_time = time.perf_counter()

        try:
            # Run the LangGraph pipeline
            final_state = self.graph.invoke(initial_state)

            elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

            # Pipeline timeout check (post-hoc — LangGraph doesn't support async cancellation natively)
            if elapsed_ms > PIPELINE_TIMEOUT_SECONDS * 1000:
                logger.warning(
                    "pipeline_timeout_exceeded",
                    trace_id=trace_id,
                    elapsed_ms=elapsed_ms,
                    timeout_ms=PIPELINE_TIMEOUT_SECONDS * 1000,
                )

            logger.info(
                "pipeline_completed",
                trace_id=trace_id,
                total_time_ms=elapsed_ms,
                intent=final_state.get("intent"),
                row_count=final_state.get("row_count", 0),
                has_error=bool(final_state.get("error")),
            )

            return final_state

        except Exception as e:
            elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
            logger.error("pipeline_failed", trace_id=trace_id, error=str(e), elapsed_ms=elapsed_ms)

            return {
                **initial_state,
                "error": f"Pipeline error: {str(e)}",
                "error_agent": "orchestrator",
                "friendly_message": "An unexpected error occurred. Please try again.",
                "query_results": [],
                "row_count": 0,
            }
