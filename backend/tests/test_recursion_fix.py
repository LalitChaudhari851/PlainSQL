import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.agents.sql_validation import route_validation
from app.agents.sql_generation import sql_generation_node
from app.agents.orchestrator import AgentOrchestrator
from unittest.mock import MagicMock, patch

@patch('app.prompts.few_shot.DynamicFewShotSelector.select', return_value=[])
def test_sql_generation_resets_validation_state(mock_select):
    # Mock LLM router
    llm_router = MagicMock()
    llm_router.generate.return_value = '{"sql": "SELECT * FROM employees;", "explanation": "explanation", "message": "message"}'
    
    # State has stale validation errors and is_valid=False
    state = {
        "user_query": "query",
        "relevant_schema": "schema",
        "retry_count": 1,
        "is_valid": False,
        "validation_errors": ["Some error"],
        "sanitized_sql": "SELECT 1;",
        "trace_id": "test",
    }
    
    res = sql_generation_node(state, llm_router)
    # The return value from sql_generation_node must reset is_valid, validation_errors, and sanitized_sql
    assert res["is_valid"] is None
    assert res["validation_errors"] == []
    assert res["sanitized_sql"] == ""
    assert res["generated_sql"] == "SELECT * FROM employees;"

def test_route_validation_hard_cap():
    # Normal retry
    state_retry = {"is_valid": False, "retry_count": 2}
    assert route_validation(state_retry) == "retry"
    
    # Blocked at max 3 retries
    state_blocked = {"is_valid": False, "retry_count": 3}
    assert route_validation(state_blocked) == "blocked"
    
    # Hard cap at 10 retries
    state_hard_cap = {"is_valid": False, "retry_count": 10}
    assert route_validation(state_hard_cap) == "blocked"
    
    # Hard cap at 11 retries
    state_hard_cap_11 = {"is_valid": False, "retry_count": 11}
    assert route_validation(state_hard_cap_11) == "blocked"

@patch('app.agents.orchestrator.AgentOrchestrator._init_semantic_cache', return_value=None)
def test_orchestrator_safe_execute_preserves_retry_count(mock_init_cache):
    # Create orchestrator with mock deps
    orchestrator = AgentOrchestrator(
        llm_router=MagicMock(),
        rag_retriever=MagicMock(),
        db_pool=MagicMock()
    )
    
    state = {"retry_count": 2, "trace_id": "test"}
    
    def failing_node(state):
        raise ValueError("Simulated failure")
        
    res = orchestrator._safe_execute("critical_agent", failing_node, state)
    
    assert "error" in res
    assert res["retry_count"] == 2
