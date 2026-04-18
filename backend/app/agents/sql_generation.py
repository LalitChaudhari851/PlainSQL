"""
SQL Generation Agent — Generates SQL queries from natural language using LLM.
Receives schema context from RAG and produces structured SQL output.
"""

import json
import re
import structlog

from app.agents.state import AgentState
from app.prompts.registry import get_prompt_registry

logger = structlog.get_logger()


def sql_generation_node(state: AgentState, llm_router) -> dict:
    """
    Generate SQL query from the user's question using schema context.
    Outputs structured JSON with sql, explanation, and friendly message.
    """
    user_query = state["user_query"]
    context = state.get("relevant_schema", "")
    history = state.get("conversation_history", [])
    intent = state.get("intent", "data_query")
    retry_count = state.get("retry_count", 0)
    validation_errors = state.get("validation_errors", [])
    trace_id = state.get("trace_id", "unknown")

    logger.info("agent_started", agent="sql_generation", trace_id=trace_id, retry=retry_count)

    # Build conversation history context
    history_text = ""
    if history:
        recent = history[-3:]  # Last 3 exchanges
        history_text = "PREVIOUS CONVERSATION:\n"
        for h in recent:
            history_text += f"User: {h.get('user', '')}\nSQL: {h.get('sql', '')}\n"

    # If this is a retry, include the validation errors for self-correction
    retry_context = ""
    if retry_count > 0 and validation_errors:
        retry_context = f"""
⚠️ YOUR PREVIOUS SQL WAS REJECTED. Fix these issues:
{chr(10).join(f'  - {err}' for err in validation_errors)}

Previous attempt: {state.get('generated_sql', 'N/A')}
Generate a corrected version.
"""

    prompt_template = get_prompt_registry().get("sql_generation")
    messages = prompt_template.render(
        schema_context=context,
        history_context=history_text,
        retry_context=retry_context,
        user_query=user_query,
    )

    try:
        # Use higher quality model for complex queries
        model_pref = "accurate" if state.get("complexity") == "complex" else "default"
        response = llm_router.generate(messages, model_preference=model_pref, max_tokens=1024, temperature=0.1)

        # Parse structured response
        sql_query, explanation, message = _parse_llm_response(response)

        if not sql_query:
            logger.warning("empty_sql_generated", response_preview=response[:200])
            return {
                "generated_sql": "",
                "sql_explanation": "Failed to generate SQL",
                "friendly_message": "I couldn't generate a query for that request. Could you rephrase?",
                "error": "Empty SQL output from LLM",
                "error_agent": "sql_generation",
            }

        # Clean SQL
        sql_query = _clean_sql(sql_query)

        logger.info("sql_generated", sql_length=len(sql_query), retry=retry_count)

        return {
            "generated_sql": sql_query,
            "sql_explanation": explanation,
            "friendly_message": message,
        }

    except Exception as e:
        logger.error("sql_generation_failed", error=str(e))
        return {
            "generated_sql": "",
            "sql_explanation": "",
            "friendly_message": "An error occurred while generating the query.",
            "error": f"SQL generation failed: {str(e)}",
            "error_agent": "sql_generation",
        }


def _parse_llm_response(response: str) -> tuple[str, str, str]:
    """Parse the LLM response, handling both JSON and raw SQL formats."""
    sql_query = ""
    explanation = "Query generated successfully."
    message = "Here are your results."

    try:
        # Try JSON parsing first
        clean_json = re.sub(r"```json|```", "", response).strip()
        data = json.loads(clean_json)
        sql_query = data.get("sql", "")
        message = data.get("message", message)
        explanation = data.get("explanation", explanation)
    except (json.JSONDecodeError, ValueError):
        # Fallback: extract SQL from raw text
        # Try to find SELECT...FROM...; (requires FROM to ensure it's SQL, not prose)
        match = re.search(r"((?:WITH\s+\w+\s+AS\s*\([\s\S]+?\)\s*)?SELECT\s+[\s\S]+?\sFROM\s[\s\S]+?;)", response, re.IGNORECASE)
        if match:
            sql_query = match.group(1)
        else:
            # Try without semicolon but still require FROM
            match = re.search(r"((?:WITH\s+\w+\s+AS\s*\([\s\S]+?\)\s*)?SELECT\s+[\s\S]+?\sFROM\s[\s\S]+?)(?:\n\n|$)", response, re.IGNORECASE)
            if match:
                sql_query = match.group(1)

    return sql_query, explanation, message


def _clean_sql(sql: str) -> str:
    """Clean and normalize generated SQL."""
    # Remove markdown formatting
    sql = re.sub(r"```sql|```", "", sql, flags=re.IGNORECASE).strip()
    # Normalize whitespace
    sql = " ".join(sql.split())
    # Ensure trailing semicolon
    if sql and not sql.endswith(";"):
        sql += ";"
    return sql
