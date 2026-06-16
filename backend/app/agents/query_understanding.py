"""
Query Understanding Agent - classifies user intent and extracts entities.
First agent in the pipeline. Determines routing for the rest of the graph.
"""

import structlog

from app.agents.intent_classifier import build_chat_response, classify_intent
from app.agents.state import AgentState

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
    Extract table names and relevant entities based on keyword matching
    covering all 22 database tables and their common business synonyms.
    """
    query_lower = query.lower()
    
    # Map keywords and synonyms to the actual database table names
    keyword_map = {
        "subscriptions": ["subscriptions"],
        "subscription": ["subscriptions"],
        "arr": ["subscriptions"],
        "mrr": ["subscriptions"],
        "nrr": ["subscriptions", "accounts"],
        "retention": ["subscriptions", "accounts"],
        "contracted_arr": ["subscriptions"],
        "contract": ["subscriptions"],
        
        "invoices": ["invoices"],
        "invoice": ["invoices"],
        "billing": ["invoices"],
        "bill": ["invoices"],
        "revenue": ["invoices", "subscriptions"],
        
        "payments": ["payments"],
        "payment": ["payments"],
        "paid": ["payments"],
        "transaction": ["payments"],
        
        "opportunities": ["opportunities"],
        "opportunity": ["opportunities"],
        "deal": ["opportunities"],
        "deals": ["opportunities"],
        "pipeline": ["opportunities"],
        "forecast": ["opportunities"],
        
        "support_tickets": ["support_tickets"],
        "ticket": ["support_tickets"],
        "tickets": ["support_tickets"],
        "support": ["support_tickets"],
        "csat": ["support_tickets"],
        
        "ticket_events": ["ticket_events"],
        "sla": ["ticket_events", "support_tickets"],
        "breach": ["ticket_events"],
        
        "incidents": ["incidents"],
        "incident": ["incidents"],
        "outage": ["incidents"],
        "downtime": ["incidents"],
        "severity": ["incidents"],
        
        "workspaces": ["workspaces"],
        "workspace": ["workspaces"],
        "environment": ["workspaces"],
        "env": ["workspaces"],
        
        "workspace_users": ["workspace_users"],
        "workspace_user": ["workspace_users"],
        
        "accounts": ["accounts"],
        "account": ["accounts"],
        "customer": ["accounts"],
        "customers": ["accounts"],
        "segment": ["accounts"],
        "industry": ["accounts"],
        "region": ["accounts"],
        "health": ["accounts"],
        "churn": ["accounts"],
        
        "contacts": ["contacts"],
        "contact": ["contacts"],
        "sponsor": ["contacts"],
        
        "employees": ["employees"],
        "employee": ["employees"],
        "manager": ["employees"],
        "rep": ["employees"],
        "staff": ["employees"],
        "hire": ["employees"],
        "salary": ["employees"],
        "salaries": ["employees"],
        
        "departments": ["departments"],
        "department": ["departments"],
        "cost center": ["departments"],
        
        "plans": ["plans"],
        "plan": ["plans"],
        "pricing": ["plans"],
        
        "products": ["products"],
        "product": ["products"],
        
        "feature_catalog": ["feature_catalog"],
        "feature": ["feature_catalog", "products"],
        "features": ["feature_catalog", "products"],
        
        "product_usage_daily": ["product_usage_daily"],
        "usage": ["product_usage_daily"],
        "queries": ["product_usage_daily"],
        "latency": ["product_usage_daily"],
        
        "query_audit_log": ["query_audit_log"],
        "audit": ["query_audit_log"],
        
        "users": ["workspace_users", "plainsql_users"],
        "user": ["workspace_users", "plainsql_users"],
    }
    
    matched_tables = set()
    
    # 1. First, check for exact/partial word matching
    import re
    words = re.findall(r"[a-z_0-9]+", query_lower)
    
    for word in words:
        if word in keyword_map:
            matched_tables.update(keyword_map[word])
            
    # 2. Check for multi-word phrases (like "cost center", "ticket event")
    for key, tables in keyword_map.items():
        if " " in key and key in query_lower:
            matched_tables.update(tables)
            
    # 3. Fallback: check if the actual table name is in the query text directly
    all_table_names = [
        "departments", "employees", "plans", "products", "feature_catalog",
        "accounts", "contacts", "workspaces", "workspace_users", "subscriptions",
        "invoices", "payments", "opportunities", "product_usage_daily", "support_tickets",
        "ticket_events", "incidents", "query_audit_log", "query_feedback", "conversations",
        "messages", "plainsql_users"
    ]
    for table in all_table_names:
        if table in query_lower:
            matched_tables.add(table)
            
    return list(matched_tables)


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
