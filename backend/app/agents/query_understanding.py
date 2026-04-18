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
            "friendly_message": build_chat_response(user_query),
        }

    if classification.intent == "ambiguous":
        logger.info("intent_classified", intent="ambiguous", method=classification.reason)
        return {
            "intent": "ambiguous",
            "route_intent": "chat",
            "entities": _extract_entities_basic(user_query),
            "complexity": "simple",
            "friendly_message": _build_ambiguous_response(user_query),
        }

    if classification.route_intent == "meta_query":
        logger.info("intent_classified", intent="sql", route_intent="meta_query", method=classification.reason)
        return {
            "intent": "sql",
            "route_intent": "meta_query",
            "entities": _extract_entities_basic(user_query),
            "complexity": classification.complexity,
        }

    # Optional LLM refinement for SQL sub-intent only. The chat/sql decision has
    # already been made.
    try:
        prompt_template = get_prompt_registry().get("query_classification")
        messages = prompt_template.render(user_query=user_query)
        response = llm_router.generate(messages, model_preference="fast")

        clean_json = re.sub(r"```json|```", "", response).strip()
        parsed = json.loads(clean_json)

        intent = parsed.get("route_intent") or parsed.get("intent", classification.route_intent)
        entities = parsed.get("entities", [])
        complexity = parsed.get("complexity", classification.complexity)

        # The rule-based classifier already decided this is SQL.
        # The LLM refinement should NOT override that decision back to chat —
        # doing so would allow prompt injection to bypass the SQL pipeline.
        valid_intents = {"data_query", "aggregation", "comparison", "explanation"}
        if intent not in valid_intents:
            intent = classification.route_intent if classification.route_intent in valid_intents else "data_query"

        logger.info(
            "intent_classified",
            intent="sql",
            route_intent=intent,
            entities=entities,
            complexity=complexity,
            method="llm",
        )

        return {
            "intent": "sql",
            "route_intent": intent,
            "entities": entities,
            "complexity": complexity,
        }

    except Exception as e:
        logger.warning("classification_failed", error=str(e), fallback="heuristic")
        return {
            "intent": "sql",
            "route_intent": classification.route_intent,
            "entities": _extract_entities_basic(user_query),
            "complexity": classification.complexity,
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
