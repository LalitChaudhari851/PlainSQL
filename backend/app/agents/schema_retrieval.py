"""
Schema Retrieval Agent — Fetches relevant schema context using hybrid RAG.
Uses both vector similarity and keyword search to find the best matching tables.
"""

import structlog

from app.agents.state import AgentState

logger = structlog.get_logger()


def schema_retrieval_node(state: AgentState, rag_retriever, db_pool) -> dict:
    """
    Retrieve relevant database schema context for SQL generation.
    Uses hybrid search (vector + BM25) for best results.
    """
    user_query = state["user_query"]
    entities = state.get("entities", [])
    route_intent = state.get("route_intent", state.get("intent", "data_query"))
    trace_id = state.get("trace_id", "unknown")

    logger.info("agent_started", agent="schema_retrieval", trace_id=trace_id)

    try:
        # ── Meta query: return full schema ───────────────
        if route_intent == "meta_query":
            full_schema = db_pool.get_full_schema()
            tables = db_pool.get_tables()
            return {
                "relevant_schema": full_schema,
                "relevant_tables": tables,
            }

        # ── Hybrid RAG retrieval ─────────────────────────
        # Combine user query with extracted entities for better retrieval
        search_query = user_query
        if entities:
            search_query += " " + " ".join(entities)

        retrieved_docs = rag_retriever.retrieve(search_query, top_k=5)

        if not retrieved_docs:
            # Fallback: return full schema
            logger.warning("rag_empty_results", fallback="full_schema")
            full_schema = db_pool.get_full_schema()
            return {
                "relevant_schema": full_schema,
                "relevant_tables": db_pool.get_tables(),
            }

        # Extract table names from retrieved documents
        relevant_tables = []
        for doc in retrieved_docs:
            for line in doc.split("\n"):
                if line.startswith("Table: "):
                    table_name = line.replace("Table: ", "").strip()
                    if table_name not in relevant_tables:
                        relevant_tables.append(table_name)

        relevant_schema = "\n\n".join(retrieved_docs)

        logger.info(
            "schema_retrieved",
            tables_found=len(relevant_tables),
            tables=relevant_tables,
        )

        return {
            "relevant_schema": relevant_schema,
            "relevant_tables": relevant_tables,
        }

    except Exception as e:
        logger.error("schema_retrieval_failed", error=str(e))
        # Graceful fallback
        try:
            full_schema = db_pool.get_full_schema()
            return {
                "relevant_schema": full_schema,
                "relevant_tables": db_pool.get_tables(),
            }
        except Exception:
            return {
                "relevant_schema": "Schema unavailable",
                "relevant_tables": [],
                "error": f"Schema retrieval failed: {str(e)}",
                "error_agent": "schema_retrieval",
            }
