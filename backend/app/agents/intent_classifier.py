"""
Intent classifier for routing user input before SQL generation.

Clear chat messages return fast and never touch schema retrieval or SQL
generation. Database-shaped requests continue through the existing SQL path.
"""

from dataclasses import dataclass
from typing import Literal
import re


IntentKind = Literal["chat", "sql"]
RouteIntent = Literal["chat", "meta_query", "data_query", "aggregation", "comparison", "explanation"]


@dataclass(frozen=True)
class IntentClassification:
    intent: IntentKind
    route_intent: RouteIntent
    complexity: Literal["simple", "moderate", "complex"] = "simple"
    reason: str = "heuristic"


GREETING_PATTERNS = {
    "hello", "hi", "hey", "good morning", "good afternoon", "good evening",
    "hola", "sup", "what's up", "howdy", "yo", "greetings", "namaste", "bonjour",
}

CHAT_EXACT = {
    "thanks", "thank you", "thank u", "thx", "ty", "ok", "okay", "cool",
    "great", "nice", "awesome", "got it", "bye", "goodbye", "see you",
    "see ya", "later", "yes", "no", "yep", "nope", "sure", "nah",
    "help", "help me", "what can you do", "who are you", "what are you",
    "how are you", "how do you work", "how does this work",
    "tell me about yourself", "what is this", "what is plainsql",
}

CHAT_PREFIXES = {
    "thanks for", "thank you for", "can you help", "please help",
    "i need help", "i don't understand", "what do you do", "what can i ask",
    "how do i use", "tell me about you", "who made you", "are you",
    "tell me a joke", "how is the weather",
}

DATA_SIGNAL_PHRASES = {
    "how many", "how much", "group by", "order by", "by region",
    "by department", "by category", "by month", "by year",
}

DATA_SIGNAL_WORDS = {
    "show", "list", "find", "get", "select", "count", "total", "average",
    "avg", "sum", "top", "bottom", "highest", "lowest", "maximum",
    "minimum", "max", "min", "most", "least", "revenue", "sales",
    "salary", "salaries", "price", "stock", "spend", "employees",
    "employee", "customers", "customer", "products", "product",
    "departments", "department", "orders", "order", "table", "tables",
    "column", "columns", "rows", "records", "where", "between", "greater",
    "less", "equal", "filter", "sort", "compare", "vs", "versus", "per",
    "each", "all", "every", "join", "query", "data", "database", "report",
}

META_KEYWORDS = {
    "what tables", "show tables", "list tables", "what columns",
    "describe table", "schema", "what database",
}
AGGREGATION_KEYWORDS = {
    "count", "total", "sum", "average", "avg", "minimum", "maximum",
    "min", "max", "group by", "by region", "by department", "by category",
    "by month", "by year",
}
COMPARISON_KEYWORDS = {"compare", " vs ", " versus ", "between"}
EXPLANATION_KEYWORDS = {"explain this", "why did", "what does this query"}


def classify_intent(user_query: str) -> IntentClassification:
    """Classify input as chat or SQL and choose the downstream graph route."""
    query = _normalize(user_query)
    if not query:
        return IntentClassification("chat", "chat", reason="empty")

    has_data_signal = _has_data_signal(query)

    # Mixed inputs like "hi, show top 5 employees" should still be SQL.
    if has_data_signal:
        return IntentClassification(
            "sql",
            _classify_sql_route(query),
            complexity=_estimate_complexity(query),
            reason="data_signal",
        )

    if _is_chat(query):
        return IntentClassification("chat", "chat", reason="chat_signal")

    if len(query.split()) <= 4:
        return IntentClassification("chat", "chat", reason="short_without_data_signal")

    # Preserve existing text-to-SQL behavior for ambiguous analytical requests.
    return IntentClassification(
        "sql",
        _classify_sql_route(query),
        complexity=_estimate_complexity(query),
        reason="ambiguous_default_sql",
    )


def build_chat_response(user_query: str) -> str:
    """Return a simple conversational response without invoking SQL generation."""
    query = _normalize(user_query)

    if any(kw in query for kw in ("what can you do", "what do you do", "how do you work", "how does this work", "help", "what can i ask", "how do i use")):
        return (
            "I'm PlainSQL, your data assistant. Ask me a database question in plain "
            "English and I can generate safe read-only SQL, run it, and summarize the result. "
            "Try: 'Show top 5 employees by salary' or 'Total sales by region'."
        )

    if any(kw in query for kw in ("who are you", "what are you", "tell me about you", "what is plainsql", "what is this")):
        return (
            "I'm PlainSQL, an assistant for querying your database with natural language. "
            "Describe the data you want and I'll handle the SQL path."
        )

    if any(kw in query for kw in ("thanks", "thank", "thx", "ty")):
        return "You're welcome. Ask me any database question when you're ready."

    if any(kw in query for kw in ("bye", "goodbye", "see you", "see ya", "later")):
        return "Goodbye. Come back anytime you want to explore your data."

    return (
        "Hello. I can help you query your database in plain English. "
        "Try asking something like 'Show top 5 employees by salary'."
    )


def _normalize(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value.lower()).strip()
    return cleaned.strip(" \t\r\n.!?")


def _has_data_signal(query: str) -> bool:
    if any(phrase in query for phrase in DATA_SIGNAL_PHRASES):
        return True
    words = set(re.findall(r"[a-z_]+", query))
    return bool(words.intersection(DATA_SIGNAL_WORDS))


def _is_chat(query: str) -> bool:
    if query in CHAT_EXACT or query in GREETING_PATTERNS:
        return True
    if any(query.startswith(prefix) for prefix in CHAT_PREFIXES):
        return True
    return any(query == greeting or query.startswith(greeting + " ") for greeting in GREETING_PATTERNS)


def _classify_sql_route(query: str) -> RouteIntent:
    if any(kw in query for kw in META_KEYWORDS):
        return "meta_query"
    if any(kw in query for kw in COMPARISON_KEYWORDS):
        return "comparison"
    if any(kw in query for kw in EXPLANATION_KEYWORDS):
        return "explanation"
    if any(kw in query for kw in AGGREGATION_KEYWORDS):
        return "aggregation"
    return "data_query"


def _estimate_complexity(query: str) -> Literal["simple", "moderate", "complex"]:
    if any(kw in query for kw in ("subquery", "window", "rank", "running total", "percentile")):
        return "complex"
    if any(kw in query for kw in ("join", "compare", " vs ", " versus ", "group by", " by ")):
        return "moderate"
    return "simple"
