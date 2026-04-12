"""
Tests for the Agent State and Query Understanding.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.agents.intent_classifier import classify_intent
from app.agents.query_understanding import query_understanding_node, _extract_entities_basic


class TestQueryUnderstanding:
    """Test intent classification and entity extraction."""

    class MockLLMRouter:
        def generate(self, messages, **kwargs):
            return '{"intent": "data_query", "entities": ["employees"], "complexity": "simple"}'

    class FailingLLMRouter:
        def generate(self, messages, **kwargs):
            raise AssertionError("chat inputs should not call the LLM")

    def test_greeting_detection(self):
        state = {"user_query": "hello", "trace_id": "test"}
        result = query_understanding_node(state, self.FailingLLMRouter())
        assert result["intent"] == "chat"
        assert result["route_intent"] == "chat"
        assert result["friendly_message"]

    def test_greeting_with_exclamation(self):
        state = {"user_query": "Hi!", "trace_id": "test"}
        result = query_understanding_node(state, self.FailingLLMRouter())
        assert result["intent"] == "chat"
        assert result["route_intent"] == "chat"

    def test_meta_query_detection(self):
        state = {"user_query": "What tables are in the database?", "trace_id": "test"}
        result = query_understanding_node(state, self.MockLLMRouter())
        assert result["intent"] == "sql"
        assert result["route_intent"] == "meta_query"

    def test_show_tables_detection(self):
        state = {"user_query": "show tables", "trace_id": "test"}
        result = query_understanding_node(state, self.MockLLMRouter())
        assert result["intent"] == "sql"
        assert result["route_intent"] == "meta_query"

    def test_data_query_via_llm(self):
        state = {"user_query": "Show top 5 employees by salary", "trace_id": "test"}
        result = query_understanding_node(state, self.MockLLMRouter())
        assert result["intent"] == "sql"
        assert result["route_intent"] == "data_query"

    def test_capability_question_is_chat(self):
        result = classify_intent("what can you do?")
        assert result.intent == "chat"
        assert result.route_intent == "chat"

    def test_thanks_is_chat(self):
        result = classify_intent("thanks")
        assert result.intent == "chat"
        assert result.route_intent == "chat"

    def test_mixed_greeting_data_query_is_sql(self):
        result = classify_intent("hi, show top 5 employees by salary")
        assert result.intent == "sql"
        assert result.route_intent in {"data_query", "aggregation"}

    def test_entity_extraction_basic(self):
        entities = _extract_entities_basic("Show me employee salaries by department")
        assert "employee" in entities or "employees" in entities
        assert "department" in entities or "departments" in entities

    def test_no_entities_found(self):
        entities = _extract_entities_basic("How is the weather?")
        assert len(entities) == 0
