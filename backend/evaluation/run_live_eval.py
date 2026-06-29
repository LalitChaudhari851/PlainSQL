"""
PlainSQL Live Evaluation — Comprehensive system evaluation.

This script runs three evaluation tiers:
  1. COMPONENT EVAL — Tests SQL validation, intent classification, prompt generation,
     and structural metrics LOCALLY (no external services needed)
  2. LLM EVAL — Tests SQL generation quality against the full dataset via HuggingFace API
     (requires HUGGINGFACEHUB_API_TOKEN in .env)
  3. EXECUTION EVAL — Full end-to-end with DB execution accuracy
     (requires DB_URI with valid credentials in .env)

Usage:
    cd backend
    python -m evaluation.run_live_eval              # Component-only (always works)
    python -m evaluation.run_live_eval --with-llm   # + LLM generation
    python -m evaluation.run_live_eval --full        # + DB execution accuracy

Results saved to evaluation/results/live_eval_<timestamp>.json
"""

import json
import os
import sys
import time
import argparse
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from evaluation.runner import EvalMetrics, HallucinationDetector


# ── Schema definition (matches the chatbot DB) ──────────────
KNOWN_TABLES = {"employees", "departments", "products", "customers", "sales"}
KNOWN_COLUMNS = {
    "employees": {"id", "name", "salary", "department_id", "role", "hire_date"},
    "departments": {"id", "name", "budget", "location"},
    "products": {"id", "name", "category", "price", "stock_quantity"},
    "customers": {"id", "name", "company", "region", "join_date"},
    "sales": {"sale_id", "employee_id", "customer_id", "product_id", "total_amount", "quantity", "sale_date"},
}


def load_dataset():
    """Load the evaluation dataset."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "datasets", "train.json")
    with open(path) as f:
        return json.load(f)


# ══════════════════════════════════════════════════════════════
#  TIER 1: COMPONENT EVALUATION (no external deps)
# ══════════════════════════════════════════════════════════════

def run_component_eval(dataset):
    """Evaluate SQL validation, intent classification, prompt registry, and metrics."""
    print("\n" + "=" * 65)
    print("  TIER 1: COMPONENT EVALUATION")
    print("  Tests: SQL Validator, Intent Classifier, Prompt Registry, Guardrails")
    print("=" * 65)

    results = {
        "sql_validation": eval_sql_validation(dataset),
        "intent_classification": eval_intent_classification(),
        "prompt_registry": eval_prompt_registry(dataset),
        "guardrails": eval_guardrails(dataset),
        "structural_metrics": eval_structural_metrics(dataset),
    }

    passed = sum(1 for v in results.values() if v["status"] == "PASS")
    total = len(results)
    print(f"\n  Component Results: {passed}/{total} PASS")
    return results


def eval_sql_validation(dataset):
    """Verify SQL validator correctly blocks dangerous SQL and allows valid SQL."""
    from app.agents.sql_validation import sql_validation_node

    dangerous_queries = [
        "DROP TABLE employees;",
        "DELETE FROM employees WHERE id = 1;",
        "UPDATE employees SET salary = 0;",
        "INSERT INTO employees (name) VALUES ('test');",
        "SELECT * FROM employees; DROP TABLE employees;",
        "SELECT SLEEP(10);",
        "SELECT * FROM employees UNION ALL SELECT username, password FROM information_schema.columns",
    ]

    safe_queries = [item["expected_sql"] for item in dataset]

    blocked = 0
    for dq in dangerous_queries:
        state = {"generated_sql": dq, "retry_count": 0, "trace_id": "eval"}
        result = sql_validation_node(state)
        if not result["is_valid"]:
            blocked += 1

    allowed = 0
    for sq in safe_queries:
        state = {"generated_sql": sq, "retry_count": 0, "trace_id": "eval"}
        result = sql_validation_node(state)
        if result["is_valid"]:
            allowed += 1

    block_rate = blocked / len(dangerous_queries) * 100
    allow_rate = allowed / len(safe_queries) * 100

    print("\n  [SQL Validation]")
    print(f"    Dangerous queries blocked: {blocked}/{len(dangerous_queries)} ({block_rate:.0f}%)")
    print(f"    Safe queries allowed:      {allowed}/{len(safe_queries)} ({allow_rate:.0f}%)")

    status = "PASS" if block_rate >= 85 and allow_rate >= 90 else "FAIL"
    return {
        "status": status,
        "dangerous_blocked": f"{blocked}/{len(dangerous_queries)}",
        "safe_allowed": f"{allowed}/{len(safe_queries)}",
        "block_rate": block_rate,
        "allow_rate": allow_rate,
    }


def eval_intent_classification():
    """Evaluate intent classification accuracy on known inputs."""
    from app.agents.query_understanding import query_understanding_node

    test_cases = [
        # (input, expected_route_intent_set)
        ("Hello!", {"chat"}),
        ("Hi there, how are you?", {"chat"}),
        ("What can you do?", {"chat"}),
        ("Thanks!", {"chat"}),
        ("What tables are in the database?", {"meta_query"}),
        ("Show me the schema", {"meta_query"}),
        ("Describe the employees table", {"meta_query"}),
        ("Show top 5 employees by salary", {"data_query", "aggregation"}),
        ("Total sales revenue by region", {"aggregation", "data_query"}),
        ("Compare sales between Q1 and Q2", {"comparison", "data_query", "aggregation"}),
        ("Which department has the highest average salary?", {"aggregation", "data_query"}),
        ("List all products", {"data_query"}),
    ]

    correct = 0
    for query, expected_intents in test_cases:
        state = {"user_query": query, "conversation_history": []}
        result = query_understanding_node(state, llm_router=None)
        actual = result.get("route_intent", result.get("intent", "unknown"))
        if actual in expected_intents:
            correct += 1
        else:
            print(f"    WARN '{query}' -> {actual} (expected one of {expected_intents})")

    accuracy = correct / len(test_cases) * 100
    print("\n  [Intent Classification]")
    print(f"    Accuracy: {correct}/{len(test_cases)} ({accuracy:.0f}%)")

    status = "PASS" if accuracy >= 75 else "FAIL"
    return {"status": status, "accuracy": accuracy, "correct": correct, "total": len(test_cases)}


def eval_prompt_registry(dataset):
    """Verify prompt templates render correctly for all query types."""
    from app.prompts.registry import PromptRegistry

    registry = PromptRegistry()
    templates = registry.list_templates()

    required = ["query_classification", "sql_generation", "sql_explanation"]
    missing = [t for t in required if t not in templates]

    # Test rendering
    render_ok = 0
    for item in dataset[:5]:
        try:
            template = registry.get("sql_generation")
            messages = template.render(
                schema_context="CREATE TABLE employees (id INT, name VARCHAR(100), salary DECIMAL);",
                history_context="",
                retry_context="",
                user_query=item["question"],
            )
            if len(messages) >= 2 and item["question"] in messages[-1]["content"]:
                render_ok += 1
        except Exception:
            pass

    print("\n  [Prompt Registry]")
    print(f"    Templates found: {len(templates)} (required: {len(required)}, missing: {len(missing)})")
    print(f"    Render test: {render_ok}/5 OK")

    status = "PASS" if not missing and render_ok >= 4 else "FAIL"
    return {"status": status, "templates": len(templates), "missing": missing, "render_ok": render_ok}


def eval_guardrails(dataset):
    """Test output guardrails catch hallucinated references."""
    from app.agents.guardrails import OutputGuardrail

    guardrail = OutputGuardrail(known_tables=KNOWN_TABLES, known_columns=KNOWN_COLUMNS)

    # Valid queries should have zero or few warnings
    valid_pass = 0
    for item in dataset:
        warnings = guardrail.validate_sql_references(item["expected_sql"])
        # Allow up to 2 warnings (aliases get flagged as false positives)
        if len(warnings) <= 2:
            valid_pass += 1

    # Hallucinated queries should be caught
    hallucinated_sqls = [
        "SELECT * FROM nonexistent_table",
        "SELECT fake_column FROM employees",
        "SELECT e.name FROM employees e JOIN ghost_table g ON e.id = g.emp_id",
    ]
    caught = sum(1 for sql in hallucinated_sqls if guardrail.validate_sql_references(sql))

    print("\n  [Output Guardrails]")
    print(f"    Valid SQL accepted: {valid_pass}/{len(dataset)}")
    print(f"    Hallucinations caught: {caught}/{len(hallucinated_sqls)}")

    status = "PASS" if valid_pass >= len(dataset) * 0.8 and caught >= 2 else "FAIL"
    return {
        "status": status,
        "valid_accepted": f"{valid_pass}/{len(dataset)}",
        "hallucinations_caught": f"{caught}/{len(hallucinated_sqls)}",
    }


def eval_structural_metrics(dataset):
    """Validate structural similarity metric calibration."""
    metrics = EvalMetrics()
    halluc = HallucinationDetector(KNOWN_TABLES, KNOWN_COLUMNS)

    # Self-similarity should be 1.0
    self_sim_scores = []
    for item in dataset:
        score = metrics.structural_similarity(item["expected_sql"], item["expected_sql"])
        self_sim_scores.append(score)

    avg_self_sim = sum(self_sim_scores) / len(self_sim_scores)
    perfect_self = sum(1 for s in self_sim_scores if s == 1.0)

    # Hallucination detection on expected SQL should be minimal
    total_halluc = sum(len(halluc.detect(item["expected_sql"])) for item in dataset)

    print("\n  [Structural Metrics Calibration]")
    print(f"    Self-similarity = 1.0: {perfect_self}/{len(dataset)}")
    print(f"    Avg self-similarity:   {avg_self_sim:.3f}")
    print(f"    False-positive hallucinations on gold SQL: {total_halluc}")

    status = "PASS" if avg_self_sim >= 0.99 and perfect_self >= len(dataset) * 0.95 else "FAIL"
    return {
        "status": status,
        "avg_self_similarity": avg_self_sim,
        "perfect_self_match": f"{perfect_self}/{len(dataset)}",
        "false_positive_hallucinations": total_halluc,
    }


# ══════════════════════════════════════════════════════════════
#  TIER 2: LLM EVALUATION (requires HF token)
# ══════════════════════════════════════════════════════════════

def run_llm_eval(dataset):
    """Generate SQL via the real LLM and measure structural similarity + exact match."""
    print("\n" + "=" * 65)
    print("  TIER 2: LLM SQL GENERATION EVALUATION")
    print("  Running all 35 queries through the real LLM pipeline...")
    print("=" * 65)

    from dotenv import load_dotenv
    load_dotenv()

    from app.config import get_settings
    from app.llm.router import ModelRouter
    from app.rag.retriever import HybridRetriever
    from app.db.connection import DatabasePool
    from app.agents.orchestrator import AgentOrchestrator

    settings = get_settings()

    # Connect to DB for schema retrieval (needed by RAG)
    db_pool = DatabasePool(settings.DB_URI)
    llm_router = ModelRouter({
        "default_provider": settings.DEFAULT_LLM_PROVIDER,
        "huggingface_token": settings.HUGGINGFACEHUB_API_TOKEN,
        "huggingface_model": settings.DEFAULT_MODEL,
    })
    rag = HybridRetriever(db_pool, settings.CHROMA_PERSIST_DIR)
    orchestrator = AgentOrchestrator(llm_router, rag, db_pool)

    metrics_calc = EvalMetrics()
    halluc = HallucinationDetector(KNOWN_TABLES, KNOWN_COLUMNS)

    results = []
    exact_matches = 0
    exec_matches = 0
    latencies = []

    for i, item in enumerate(dataset, 1):
        print(f"\n  [{i}/{len(dataset)}] {item['question']}")
        start = time.perf_counter()

        try:
            state = orchestrator.process_query(user_query=item["question"])
            elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
            latencies.append(elapsed_ms)

            gen_sql = state.get("sanitized_sql") or state.get("generated_sql", "")
            is_exact = metrics_calc.exact_match(gen_sql, item["expected_sql"]) if gen_sql else False
            struct_sim = metrics_calc.structural_similarity(gen_sql, item["expected_sql"]) if gen_sql else 0.0
            hallucinations = halluc.detect(gen_sql) if gen_sql else []

            if is_exact:
                exact_matches += 1

            # Execution match (if DB available)
            is_exec_match = False
            try:
                if gen_sql:
                    pred = db_pool.execute_query(gen_sql)
                    gold = db_pool.execute_query(item["expected_sql"])
                    is_exec_match = metrics_calc.execution_match(pred, gold)
                    if is_exec_match:
                        exec_matches += 1
            except Exception:
                pass

            status_icon = "[PASS]" if is_exec_match else ("[WARN]" if struct_sim >= 0.8 else "[FAIL]")
            print(f"    {status_icon} Exact:{is_exact} ExecMatch:{is_exec_match} Sim:{struct_sim:.2f} {elapsed_ms}ms")
            if gen_sql:
                print(f"    SQL: {gen_sql[:120]}...")

            results.append({
                "id": item["id"],
                "question": item["question"],
                "difficulty": item.get("difficulty", "unknown"),
                "category": item.get("category", "unknown"),
                "expected_sql": item["expected_sql"],
                "generated_sql": gen_sql,
                "exact_match": is_exact,
                "execution_match": is_exec_match,
                "structural_similarity": struct_sim,
                "hallucinations": hallucinations,
                "latency_ms": elapsed_ms,
                "error": state.get("error"),
            })

        except Exception as e:
            elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
            latencies.append(elapsed_ms)
            print(f"    [ERROR] {str(e)[:100]}")
            results.append({
                "id": item["id"],
                "question": item["question"],
                "difficulty": item.get("difficulty", "unknown"),
                "expected_sql": item["expected_sql"],
                "generated_sql": "",
                "exact_match": False,
                "execution_match": False,
                "structural_similarity": 0.0,
                "hallucinations": [],
                "latency_ms": elapsed_ms,
                "error": str(e),
            })

    total = len(dataset)
    sorted_lat = sorted(latencies)
    p50 = sorted_lat[len(sorted_lat) // 2] if sorted_lat else 0
    p95 = sorted_lat[int(len(sorted_lat) * 0.95)] if sorted_lat else 0

    summary = {
        "eval_type": "live_llm",
        "timestamp": datetime.now().isoformat(),
        "total_queries": total,
        "exact_match_rate": round(exact_matches / total * 100, 1) if total else 0,
        "execution_accuracy": round(exec_matches / total * 100, 1) if total else 0,
        "avg_structural_similarity": round(sum(r["structural_similarity"] for r in results) / total, 3) if total else 0,
        "total_hallucinations": sum(len(r["hallucinations"]) for r in results),
        "error_count": sum(1 for r in results if r.get("error")),
        "latency": {
            "avg_ms": round(sum(latencies) / len(latencies), 2) if latencies else 0,
            "p50_ms": round(p50, 2),
            "p95_ms": round(p95, 2),
            "min_ms": round(min(latencies), 2) if latencies else 0,
            "max_ms": round(max(latencies), 2) if latencies else 0,
        },
        "by_difficulty": {},
        "by_category": {},
        "results": results,
    }

    # Breakdown by difficulty
    for diff in ["easy", "medium", "hard"]:
        diff_r = [r for r in results if r["difficulty"] == diff]
        if diff_r:
            summary["by_difficulty"][diff] = {
                "count": len(diff_r),
                "exact_match_rate": round(sum(1 for r in diff_r if r["exact_match"]) / len(diff_r) * 100, 1),
                "execution_accuracy": round(sum(1 for r in diff_r if r["execution_match"]) / len(diff_r) * 100, 1),
                "avg_similarity": round(sum(r["structural_similarity"] for r in diff_r) / len(diff_r), 3),
                "avg_latency_ms": round(sum(r["latency_ms"] for r in diff_r) / len(diff_r), 2),
            }

    # Breakdown by category
    for cat in set(r.get("category", "unknown") for r in results):
        cat_r = [r for r in results if r.get("category") == cat]
        if cat_r:
            summary["by_category"][cat] = {
                "count": len(cat_r),
                "exact_match_rate": round(sum(1 for r in cat_r if r["exact_match"]) / len(cat_r) * 100, 1),
                "avg_similarity": round(sum(r["structural_similarity"] for r in cat_r) / len(cat_r), 3),
            }

    print(f"\n{'=' * 65}")
    print("  LLM EVALUATION RESULTS")
    print(f"{'=' * 65}")
    print(f"  Exact Match Rate:    {summary['exact_match_rate']}%")
    print(f"  Execution Accuracy:  {summary['execution_accuracy']}%")
    print(f"  Avg Similarity:      {summary['avg_structural_similarity']}")
    print(f"  Hallucinations:      {summary['total_hallucinations']}")
    print(f"  Errors:              {summary['error_count']}")
    print(f"  Latency p50/p95:     {summary['latency']['p50_ms']}ms / {summary['latency']['p95_ms']}ms")
    print("\n  By Difficulty:")
    for diff, data in summary["by_difficulty"].items():
        print(f"    {diff:>8}: {data['exact_match_rate']}% exact, {data['execution_accuracy']}% exec, {data['avg_similarity']:.3f} sim, {data['avg_latency_ms']}ms avg")

    return summary


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="PlainSQL Live Evaluation")
    parser.add_argument("--with-llm", action="store_true", help="Include LLM generation eval (needs HF token + DB)")
    parser.add_argument("--full", action="store_true", help="Full eval: components + LLM + execution accuracy")
    args = parser.parse_args()

    dataset = load_dataset()
    print(f"\n  Loaded {len(dataset)} evaluation queries")

    all_results = {"timestamp": datetime.now().isoformat(), "dataset_size": len(dataset)}

    # Always run component eval
    all_results["component_eval"] = run_component_eval(dataset)

    # LLM eval if requested
    if args.with_llm or args.full:
        try:
            all_results["llm_eval"] = run_llm_eval(dataset)
        except Exception as e:
            print(f"\n  [FAIL] LLM eval failed: {e}")
            print("         Make sure DB_URI and HUGGINGFACEHUB_API_TOKEN are set in .env")
            all_results["llm_eval"] = {"status": "FAILED", "error": str(e)}

    # Final summary
    print(f"\n{'=' * 65}")
    print("  EVALUATION COMPLETE")
    print(f"{'=' * 65}")

    comp = all_results["component_eval"]
    comp_pass = sum(1 for v in comp.values() if isinstance(v, dict) and v.get("status") == "PASS")
    comp_total = sum(1 for v in comp.values() if isinstance(v, dict) and "status" in v)
    print(f"  Component Tests: {comp_pass}/{comp_total} PASS")

    if "llm_eval" in all_results and isinstance(all_results["llm_eval"], dict) and "exact_match_rate" in all_results["llm_eval"]:
        llm = all_results["llm_eval"]
        print(f"  LLM Exact Match:     {llm['exact_match_rate']}%")
        print(f"  LLM Exec Accuracy:   {llm['execution_accuracy']}%")
        print(f"  LLM p50/p95 Latency: {llm['latency']['p50_ms']}ms / {llm['latency']['p95_ms']}ms")

    # Save results
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(out_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(out_dir, f"live_eval_{timestamp}.json")
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n  [SAVED] Results saved to {out_path}")

    return all_results


if __name__ == "__main__":
    main()
