"""
Prompt Registry — Versioned prompt template management.
Centralizes all LLM prompts for the system, enabling versioning,
A/B testing, and easy rollback without code changes.
"""

import structlog
from typing import Optional

logger = structlog.get_logger()


class PromptTemplate:
    """A versioned prompt template with variable substitution."""

    def __init__(self, name: str, version: str, system: str, user: str, description: str = ""):
        self.name = name
        self.version = version
        self.system = system
        self.user = user
        self.description = description

    def render(self, **kwargs) -> list[dict]:
        """Render the template with the given variables into chat messages."""
        system_content = self.system.format(**kwargs) if kwargs else self.system
        user_content = self.user.format(**kwargs) if kwargs else self.user
        return [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content},
        ]


class PromptRegistry:
    """
    Central registry for all prompt templates used in the system.
    Supports multiple versions per template for A/B testing and rollback.
    """

    def __init__(self):
        self._templates: dict[str, dict[str, PromptTemplate]] = {}
        self._active_versions: dict[str, str] = {}
        self._register_defaults()

    def register(self, template: PromptTemplate, set_active: bool = True):
        """Register a prompt template version."""
        if template.name not in self._templates:
            self._templates[template.name] = {}
        self._templates[template.name][template.version] = template
        if set_active:
            self._active_versions[template.name] = template.version
        logger.info("prompt_registered", name=template.name, version=template.version, active=set_active)

    def get(self, name: str, version: Optional[str] = None) -> PromptTemplate:
        """Get a prompt template by name (and optionally version)."""
        if name not in self._templates:
            raise KeyError(f"Prompt template '{name}' not found")
        
        target_version = version or self._active_versions.get(name)
        if not target_version or target_version not in self._templates[name]:
            raise KeyError(f"Version '{target_version}' not found for prompt '{name}'")
        
        return self._templates[name][target_version]

    def set_active_version(self, name: str, version: str):
        """Switch the active version of a prompt template."""
        if name not in self._templates or version not in self._templates[name]:
            raise KeyError(f"Template '{name}' version '{version}' not found")
        self._active_versions[name] = version
        logger.info("prompt_version_switched", name=name, version=version)

    def list_templates(self) -> dict[str, dict]:
        """List all registered templates with their versions."""
        return {
            name: {
                "active_version": self._active_versions.get(name),
                "versions": list(versions.keys()),
                "description": versions.get(self._active_versions.get(name, ""), 
                    PromptTemplate("", "", "", "")).description,
            }
            for name, versions in self._templates.items()
        }

    # ── Default Prompt Templates ──────────────────────────

    def _register_defaults(self):
        """Register all default prompt templates used by the agent pipeline."""

        # ── Query Classification ─────────────────────────
        self.register(PromptTemplate(
            name="query_classification",
            version="v1",
            description="Classifies user intent and extracts entities for routing",
            system="You are a query classifier. Respond ONLY with valid JSON.",
            user="""Classify this user message and extract any table or column names mentioned.

User Query: "{user_query}"

Respond ONLY with valid JSON:
{{
  "intent": "chat|sql",
  "route_intent": "data_query|aggregation|comparison|explanation",
  "entities": ["table_or_column_names_found"],
  "complexity": "simple|moderate|complex"
}}

Intent rules:
- "chat": Greetings, thanks, capability questions, or general conversation that does not ask for database data
- "sql": Database-related requests that need schema retrieval and SQL generation

Route intent rules for SQL messages:
- "data_query": Fetching specific rows or records (SELECT with WHERE)
- "aggregation": Counting, summing, averaging, grouping (COUNT, SUM, AVG, GROUP BY)
- "comparison": Comparing two datasets or time periods
- "explanation": Asking to explain a previous result or query

If the message is "chat", set route_intent to "data_query" and keep entities empty.

Complexity rules:
- "simple": Single table, no joins
- "moderate": 1-2 joins, some aggregation
- "complex": Multiple joins, subqueries, window functions""",
        ))

        # ── SQL Generation ───────────────────────────────
        self.register(PromptTemplate(
            name="sql_generation",
            version="v1",
            description="Generates SQL from natural language using schema context",
            system="""You are an elite SQL expert for MySQL databases.

DATABASE SCHEMA:
{schema_context}

{history_context}

{retry_context}

RULES:
1. Output ONLY valid JSON: {{ "sql": "SELECT ...", "message": "friendly explanation for user", "explanation": "technical breakdown" }}
2. Query MUST be Read-Only (SELECT or WITH...SELECT only).
3. NEVER use DELETE, DROP, UPDATE, INSERT, ALTER, TRUNCATE, or any data-modification statement.
4. Always use exact table and column names from the schema above.
5. Use proper JOINs when querying across tables — check the Relationships section.
6. Include LIMIT 100 unless the user specifically asks for all data.
7. Do NOT wrap output in markdown code blocks.
8. For aggregation queries, always include meaningful column aliases.
9. Handle NULL values appropriately in filters.
10. Only generate SQL for database-related requests. If the request is conversational or unrelated to the schema, return {{ "sql": "", "message": "I can answer chat directly, but SQL generation only handles database questions.", "explanation": "Non-database request" }}.""",
            user="{user_query}",
        ))

        # ── SQL Generation v2 (Few-Shot + Chain-of-Thought) ───
        self.register(PromptTemplate(
            name="sql_generation",
            version="v2",
            description="SQL generation with few-shot examples and chain-of-thought reasoning",
            system="""You are an elite SQL expert for MySQL databases.

DATABASE SCHEMA:
{schema_context}

{history_context}

{retry_context}

FEW-SHOT EXAMPLES:

Example 1:
Question: "Show top 5 employees by salary"
Thinking: Single table query on employees, ORDER BY salary DESC, LIMIT 5.
SQL: SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 5

Example 2:
Question: "Total sales revenue by region"
Thinking: Need to join sales with customers (for region). Aggregate SUM on sales.total_amount, GROUP BY customer.region.
SQL: SELECT c.region, SUM(s.total_amount) AS revenue FROM sales s JOIN customers c ON s.customer_id = c.id GROUP BY c.region ORDER BY revenue DESC

Example 3:
Question: "Which department has the highest average salary?"
Thinking: Join employees with departments. AVG(salary) grouped by department name. ORDER DESC, LIMIT 1 for highest.
SQL: SELECT d.name AS department, AVG(e.salary) AS avg_salary FROM employees e JOIN departments d ON e.department_id = d.id GROUP BY d.name ORDER BY avg_salary DESC LIMIT 1

INSTRUCTIONS:
1. First, reason step-by-step about which tables and joins are needed (chain-of-thought).
2. Then generate the SQL query.
3. Output ONLY valid JSON: {{ "sql": "SELECT ...", "message": "friendly explanation", "explanation": "step-by-step reasoning" }}
4. Query MUST be Read-Only (SELECT or WITH...SELECT only).
5. Always use exact table and column names from the schema.
6. Use proper JOINs when querying across tables.
7. Include LIMIT 100 unless the user asks for all data.
8. Do NOT wrap output in markdown code blocks.
9. For aggregation queries, always include meaningful column aliases.
10. Handle NULL values appropriately.""",
            user="{user_query}",
        ), set_active=False)  # v1 remains default; v2 available for A/B testing

        # ── Query Classification v2 (Enhanced) ─────────
        self.register(PromptTemplate(
            name="query_classification",
            version="v2",
            description="Enhanced intent classification with better examples and meta_query support",
            system="You are a query classifier. Respond ONLY with valid JSON.",
            user="""Classify this user message and extract any table or column names mentioned.

User Query: "{user_query}"

Examples:
- "Hello, what can you do?" → {{ "intent": "chat", "route_intent": "data_query", "entities": [], "complexity": "simple" }}
- "Show top 5 employees by salary" → {{ "intent": "sql", "route_intent": "data_query", "entities": ["employees", "salary"], "complexity": "simple" }}
- "Total revenue by region" → {{ "intent": "sql", "route_intent": "aggregation", "entities": ["sales", "region"], "complexity": "moderate" }}
- "Compare Q1 vs Q2 sales" → {{ "intent": "sql", "route_intent": "comparison", "entities": ["sales"], "complexity": "complex" }}
- "What tables do you have?" → {{ "intent": "sql", "route_intent": "meta_query", "entities": [], "complexity": "simple" }}

Respond ONLY with valid JSON:
{{
  "intent": "chat|sql",
  "route_intent": "data_query|aggregation|comparison|explanation|meta_query",
  "entities": ["table_or_column_names_found"],
  "complexity": "simple|moderate|complex"
}}""",
        ), set_active=False)  # v1 remains default; v2 available for A/B testing

        # ── SQL Explanation ───────────────────────────
        self.register(PromptTemplate(
            name="sql_explanation",
            version="v1",
            description="Explains SQL queries in plain English for non-technical users",
            system="You are a helpful data analyst explaining queries to business stakeholders. Be concise.",
            user="""Explain this SQL query in simple, non-technical English.
Write 2-3 sentences that a business person would understand.

SQL Query:
{sql}

Number of results returned: {results_count}

Explain:
1. What data it retrieves
2. Any filters or conditions applied  
3. How results are organized (sorting/grouping)

Be concise and avoid technical jargon.""",
        ))


# ── Module-level singleton ───────────────────────────────
_registry = PromptRegistry()


def get_prompt_registry() -> PromptRegistry:
    """Get the global prompt registry singleton."""
    return _registry
