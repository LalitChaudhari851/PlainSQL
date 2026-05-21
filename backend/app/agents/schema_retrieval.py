"""
Schema Retrieval Agent — Fetches relevant schema context using hybrid RAG.
Uses both vector similarity and keyword search to find the best matching tables.
retrieval_top_k is provided by the query_understanding agent and controls
how many schema documents are fetched (3=simple, 5=default, 8=complex).
"""

import structlog

from app.agents.state import AgentState

logger = structlog.get_logger()

# Valid top_k values — anything outside this range is clamped to the default.
_VALID_TOP_K = frozenset({3, 5, 8})
_DEFAULT_TOP_K = 5


def _compress_schema_to_ddl(raw_schema: str) -> str:
    """
    Compress verbose schema text into compact DDL-like format.

    Input format (from SchemaEnricher/get_full_schema):
        Table: employees
        Columns:
          - id (int) [PRIMARY KEY]
          - name (varchar(100))
          - salary (decimal(10,2))
          - department_id (int) [FOREIGN KEY]
        Relationships:
          - department_id → departments.id

    Output format:
        TABLE employees (id INT PK, name VARCHAR, salary DECIMAL, department_id INT FK)
        FK: employees.department_id → departments.id

    ~70% smaller, unambiguous column names for the LLM.
    """
    import re

    lines = raw_schema.split("\n")
    tables = []
    current_table = None
    columns = []
    fks = []

    for line in lines:
        stripped = line.strip()

        # Table header
        if stripped.startswith("Table: "):
            # Flush previous table
            if current_table and columns:
                tables.append(_format_ddl_table(current_table, columns, fks))
            current_table = stripped.replace("Table: ", "").strip()
            columns = []
            fks = []

        # Column definition
        elif stripped.startswith("- ") and "(" in stripped:
            col_match = re.match(
                r'-\s+(\w+)\s+\(([^)]+)\)\s*(.*)', stripped
            )
            if col_match:
                col_name = col_match.group(1)
                col_type = col_match.group(2).split("(")[0].upper()  # INT, VARCHAR, etc.
                flags = col_match.group(3)

                suffix = ""
                if "PRIMARY KEY" in flags:
                    suffix = " PK"
                elif "FOREIGN KEY" in flags:
                    suffix = " FK"
                columns.append(f"{col_name} {col_type}{suffix}")

        # Relationship line
        elif "→" in stripped and current_table:
            fk_match = re.match(r'-?\s*(\w+)\s*→\s*(\w+)\.(\w+)', stripped)
            if fk_match:
                fks.append(
                    f"{current_table}.{fk_match.group(1)} → {fk_match.group(2)}.{fk_match.group(3)}"
                )

    # Flush last table
    if current_table and columns:
        tables.append(_format_ddl_table(current_table, columns, fks))

    return "\n".join(tables) if tables else raw_schema  # Fallback if parsing fails


def _format_ddl_table(table_name: str, columns: list[str], fks: list[str]) -> str:
    """Format a single table as DDL-like string."""
    cols_str = ", ".join(columns)
    result = f"TABLE {table_name} ({cols_str})"
    if fks:
        result += "\n" + "\n".join(f"  FK: {fk}" for fk in fks)
    return result

def schema_retrieval_node(state: AgentState, rag_retriever, db_pool) -> dict:
    """
    Retrieve relevant database schema context for SQL generation.
    Uses hybrid search (vector + BM25) for best results.

    Reads retrieval_top_k from state (set by query_understanding agent).
    Falls back to 5 if the field is absent or invalid.
    """
    user_query = state["user_query"]
    entities = state.get("entities", [])
    route_intent = state.get("route_intent", state.get("intent", "data_query"))
    trace_id = state.get("trace_id", "unknown")

    # ── Resolve top_k from state with fallback ───────────
    raw_top_k = state.get("retrieval_top_k", _DEFAULT_TOP_K)
    top_k = raw_top_k if raw_top_k in _VALID_TOP_K else _DEFAULT_TOP_K

    logger.info(
        "agent_started",
        agent="schema_retrieval",
        trace_id=trace_id,
        retrieval_top_k=top_k,
        top_k_source="state" if raw_top_k in _VALID_TOP_K else "default_fallback",
        complexity=state.get("complexity", "unknown"),
    )

    try:
        # ── Meta query: return full schema ───────────────
        if route_intent == "meta_query":
            full_schema = db_pool.get_full_schema()
            tables = db_pool.get_tables()
            logger.info(
                "schema_retrieved",
                trace_id=trace_id,
                source="meta_query",
                tables_found=len(tables),
                top_k_used=None,
            )
            return {
                "relevant_schema": full_schema,
                "relevant_tables": tables,
                "retrieval_source": "meta_query",
            }

        # ── Hybrid RAG retrieval ─────────────────────────
        # Combine user query with extracted entities for better retrieval precision.
        search_query = user_query
        if entities:
            search_query += " " + " ".join(entities)

        # Use expanded retrieval for multi-table queries (better recall)
        if len(entities) >= 2 and hasattr(rag_retriever, 'retrieve_expanded'):
            retrieved_docs = rag_retriever.retrieve_expanded(
                search_query, entities=entities, top_k=top_k
            )
        else:
            retrieved_docs = rag_retriever.retrieve(search_query, top_k=top_k)

        if not retrieved_docs:
            # Fallback: return full schema when RAG returns nothing.
            logger.warning(
                "rag_empty_results",
                trace_id=trace_id,
                top_k_requested=top_k,
                fallback="full_schema",
            )
            full_schema = db_pool.get_full_schema()
            return {
                "relevant_schema": full_schema,
                "relevant_tables": db_pool.get_tables(),
                "retrieval_source": "full_schema_fallback",
            }

        # Extract table names from retrieved documents.
        relevant_tables = []
        for doc in retrieved_docs:
            for line in doc.split("\n"):
                if line.startswith("Table: "):
                    table_name = line.replace("Table: ", "").strip()
                    if table_name not in relevant_tables:
                        relevant_tables.append(table_name)

        # Compress schema to DDL format for smaller prompt context
        raw_schema = "\n\n".join(retrieved_docs)
        relevant_schema = _compress_schema_to_ddl(raw_schema)

        logger.info(
            "schema_retrieved",
            trace_id=trace_id,
            source=f"rag_top_k:{top_k}",
            top_k_requested=top_k,
            docs_returned=len(retrieved_docs),
            tables_found=len(relevant_tables),
            tables=relevant_tables,
            compression_ratio=round(len(relevant_schema) / max(len(raw_schema), 1), 2),
        )

        return {
            "relevant_schema": relevant_schema,
            "relevant_tables": relevant_tables,
            "retrieval_source": f"rag_top_k:{top_k}",
        }

    except Exception as e:
        logger.error("schema_retrieval_failed", trace_id=trace_id, error=str(e))
        # Graceful fallback — never crash the pipeline over a retrieval failure.
        try:
            full_schema = db_pool.get_full_schema()
            return {
                "relevant_schema": full_schema,
                "relevant_tables": db_pool.get_tables(),
                "retrieval_source": "full_schema_fallback",
            }
        except Exception:
            return {
                "relevant_schema": "Schema unavailable",
                "relevant_tables": [],
                "retrieval_source": "error",
                "error": f"Schema retrieval failed: {str(e)}",
                "error_agent": "schema_retrieval",
            }

