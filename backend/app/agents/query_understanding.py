"""
Query Understanding Agent - classifies user intent and extracts entities.
First agent in the pipeline. Determines routing for the rest of the graph.
"""

import json
import re
import structlog

from app.agents.intent_classifier import build_chat_response, classify_intent
from app.agents.state import AgentState
from app.prompts.registry import get_prompt_registry

logger = structlog.get_logger()

# ── Complexity → retrieval_top_k mapping ────────────────
# Simple queries need fewer schema docs (less noise, faster).
# Complex queries need more context to join multiple tables correctly.
_COMPLEXITY_TOP_K: dict[str, int] = {
    "simple": 3,
    "moderate": 5,
    "complex": 8,
}


def _is_chat_input(query_lower: str) -> bool:
    """Backward-compatible wrapper for the old chat fast-path helper."""
    return classify_intent(query_lower).intent == "chat"


def _build_greeting_response(user_query: str) -> dict:
    """Backward-compatible wrapper for the old chat response helper."""
    return {
        "intent": "chat",
        "route_intent": "chat",
        "entities": [],
        "complexity": "simple",
        "friendly_message": build_chat_response(user_query),
    }


def query_understanding_node(state: AgentState, llm_router) -> dict:
    """
    Classify the user's intent and extract relevant entities.

    The chat/sql decision is rule-based and happens before any LLM call so
    casual messages cannot be forced into SQL generation.
    """
    user_query = state["user_query"]
    trace_id = state.get("trace_id", "unknown")

    logger.info("agent_started", agent="query_understanding", trace_id=trace_id, query=user_query)

    classification = classify_intent(user_query)

    if classification.intent == "chat":
        logger.info("intent_classified", intent="chat", method=classification.reason)
        return {
            "intent": "chat",
            "route_intent": "chat",
            "entities": [],
            "complexity": classification.complexity,
            "retrieval_top_k": _COMPLEXITY_TOP_K.get(classification.complexity, 5),
            "friendly_message": build_chat_response(user_query),
        }

    if classification.intent == "ambiguous":
        logger.info("intent_classified", intent="ambiguous", method=classification.reason)
        return {
            "intent": "ambiguous",
            "route_intent": "chat",
            "entities": _extract_entities_basic(user_query),
            "complexity": "simple",
            "retrieval_top_k": 5,  # default — ambiguous queries don't reach schema retrieval
            "friendly_message": _build_ambiguous_response(user_query),
        }

    if classification.route_intent == "meta_query":
        logger.info("intent_classified", intent="sql", route_intent="meta_query", method=classification.reason)
        return {
            "intent": "sql",
            "route_intent": "meta_query",
            "entities": _extract_entities_basic(user_query),
            "complexity": classification.complexity,
            "retrieval_top_k": _COMPLEXITY_TOP_K.get(classification.complexity, 5),
        }

    # ── Heuristic-only classification (no LLM call) ──────────
    # The rule-based classifier already determines the correct SQL sub-intent.
    # An LLM refinement call was previously made here but added ~800ms of latency
    # without improving downstream SQL generation quality — all SQL intents
    # follow the same pipeline path (retrieve → generate → validate → execute).
    route = classification.route_intent
    entities = _extract_entities_basic(user_query)
    complexity = classification.complexity
    top_k = _COMPLEXITY_TOP_K.get(complexity, 5)

    logger.info(
        "intent_classified",
        intent="sql",
        route_intent=route,
        entities=entities,
        complexity=complexity,
        retrieval_top_k=top_k,
        method="heuristic_fast",
    )

    return {
        "intent": "sql",
        "route_intent": route,
        "entities": entities,
        "complexity": complexity,
        "retrieval_top_k": top_k,
    }


def _extract_entities_basic(query: str) -> list[str]:
    """
    Basic entity extraction without LLM - looks for common table name patterns.
    Fallback when LLM classification fails.
    """
    common_tables = [
        "employees", "employee", "departments", "department",
        "products", "product", "customers", "customer",
        "sales", "sale", "orders", "order", "users", "user",
    ]
    query_lower = query.lower()
    return [t for t in common_tables if t in query_lower]


def _build_ambiguous_response(user_query: str) -> str:
    """Return a helpful response for queries that are too vague to generate SQL."""
    entities = _extract_entities_basic(user_query)

    if entities:
        table_name = entities[0]
        return (
            f"I found a reference to **{table_name}**, but your question is a bit vague. "
            f"Could you be more specific? For example:\n\n"
            f"• \"Show all {table_name}\"\n"
            f"• \"How many {table_name} are there?\"\n"
            f"• \"Show top 5 {table_name} by name\""
        )

    return (
        "I'm not sure what data you're looking for. I can query these tables: "
        "**employees**, **departments**, **products**, **customers**, **sales**.\n\n"
        "Try asking something specific like:\n"
        "• \"Show top 5 employees by salary\"\n"
        "• \"Total sales revenue by region\"\n"
        "• \"List products with low stock\"\n"
        "• \"Which department has the highest average salary?\""
    )
