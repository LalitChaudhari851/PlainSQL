"""
SQL Explainer — Converts SQL queries into plain English explanations.
Uses LLM to generate non-technical descriptions of what a query does.
"""

import structlog
from app.prompts.registry import get_prompt_registry

logger = structlog.get_logger()


class SQLExplainer:
    """Explains SQL queries in natural language for non-technical users."""

    def __init__(self, llm_router):
        self.llm_router = llm_router

    def explain(self, sql: str, results_count: int = 0) -> str:
        """
        Generate a plain English explanation of a SQL query.
        """
        try:
            prompt_template = get_prompt_registry().get("sql_explanation")
            messages = prompt_template.render(sql=sql, results_count=results_count)
            explanation = self.llm_router.generate(messages, model_preference="fast", max_tokens=256)
            return explanation.strip()
        except Exception as e:
            logger.warning("sql_explanation_failed", error=str(e))
            return "Unable to generate explanation at this time."
