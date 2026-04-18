"""
Tests for the SQL Safety Validation Agent.
Verifies that dangerous queries are blocked and safe queries pass.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.agents.sql_validation import sql_validation_node, route_validation


class TestSQLValidation:
    """Test suite for SQL safety validation."""

    def _validate(self, sql: str) -> dict:
        """Helper: run validation on a SQL string."""
        state = {"generated_sql": sql, "retry_count": 0, "trace_id": "test"}
        return sql_validation_node(state)

    # ── Safe Queries (should PASS) ───────────────────────

    def test_simple_select(self):
        result = self._validate("SELECT * FROM employees LIMIT 10;")
        assert result["is_valid"] is True

    def test_select_with_where(self):
        result = self._validate("SELECT name, salary FROM employees WHERE salary > 50000;")
        assert result["is_valid"] is True

    def test_select_with_join(self):
        result = self._validate(
            "SELECT e.name, d.name FROM employees e JOIN departments d ON e.department_id = d.id;"
        )
        assert result["is_valid"] is True

    def test_select_with_aggregation(self):
        result = self._validate(
            "SELECT department_id, AVG(salary) as avg_sal FROM employees GROUP BY department_id;"
        )
        assert result["is_valid"] is True

    def test_with_cte(self):
        result = self._validate(
            "WITH top_emp AS (SELECT * FROM employees ORDER BY salary DESC LIMIT 5) SELECT * FROM top_emp;"
        )
        assert result["is_valid"] is True

    # ── Dangerous Queries (should BLOCK) ─────────────────

    def test_block_drop_table(self):
        result = self._validate("DROP TABLE employees;")
        assert result["is_valid"] is False
        assert any("DROP" in e.upper() for e in result["validation_errors"])

    def test_block_delete(self):
        result = self._validate("DELETE FROM employees WHERE id = 1;")
        assert result["is_valid"] is False

    def test_block_update(self):
        result = self._validate("UPDATE employees SET salary = 0;")
        assert result["is_valid"] is False

    def test_block_insert(self):
        result = self._validate("INSERT INTO employees (name) VALUES ('hacker');")
        assert result["is_valid"] is False

    def test_block_truncate(self):
        result = self._validate("TRUNCATE TABLE employees;")
        assert result["is_valid"] is False

    def test_block_multi_statement_injection(self):
        result = self._validate("SELECT * FROM employees; DROP TABLE employees;")
        assert result["is_valid"] is False

    def test_block_sleep_injection(self):
        result = self._validate("SELECT SLEEP(10);")
        assert result["is_valid"] is False

    # ── LIMIT Injection ──────────────────────────────────

    def test_limit_injection_when_missing(self):
        result = self._validate("SELECT * FROM employees;")
        assert result["is_valid"] is True
        assert "LIMIT" in result["sanitized_sql"].upper()

    def test_preserves_existing_limit(self):
        result = self._validate("SELECT * FROM employees LIMIT 5;")
        assert result["is_valid"] is True
        assert "LIMIT 5" in result["sanitized_sql"]

    # ── Route Validation ─────────────────────────────────

    def test_route_valid(self):
        state = {"is_valid": True, "retry_count": 0}
        assert route_validation(state) == "valid"

    def test_route_retry(self):
        state = {"is_valid": False, "retry_count": 1}
        assert route_validation(state) == "retry"

    def test_route_blocked_after_max_retries(self):
        state = {"is_valid": False, "retry_count": 3}
        assert route_validation(state) == "blocked"

    # ── Empty / Invalid Input ────────────────────────────

    def test_empty_query(self):
        result = self._validate("")
        assert result["is_valid"] is False

    def test_none_query(self):
        state = {"generated_sql": None, "retry_count": 0, "trace_id": "test"}
        result = sql_validation_node(state)
        assert result["is_valid"] is False
