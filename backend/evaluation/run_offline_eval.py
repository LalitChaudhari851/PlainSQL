"""
Offline Evaluation — Validates the evaluation metrics and dataset without a live DB or LLM.

This script proves the evaluation infrastructure works by:
1. Testing the exact-match and structural-similarity metrics against known SQL pairs
2. Running the hallucination detector against test cases
3. Simulating realistic "model output" variants and scoring them

Usage:
    cd backend
    python -m evaluation.run_offline_eval

This does NOT require a running database or LLM provider.
For full end-to-end evaluation with a live system, use:
    python -m evaluation.runner
"""

import json
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from evaluation.runner import EvalMetrics, HallucinationDetector


# ── Simulated Model Outputs ─────────────────────────────────
# These represent realistic variations a model might produce vs. the expected SQL.
# Used to demonstrate what "execution accuracy" looks like in practice.

SIMULATED_OUTPUTS = [
    {
        "id": "sim_001",
        "question": "Show top 5 employees by salary",
        "expected_sql": "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 5",
        "generated_sql": "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 5",
        "difficulty": "easy",
        "note": "Exact match — model produces identical SQL",
    },
    {
        "id": "sim_002",
        "question": "Show top 5 employees by salary",
        "expected_sql": "SELECT name, salary FROM employees ORDER BY salary DESC LIMIT 5",
        "generated_sql": "SELECT e.name, e.salary FROM employees e ORDER BY e.salary DESC LIMIT 5",
        "difficulty": "easy",
        "note": "Alias variation — semantically equivalent, not exact match",
    },
    {
        "id": "sim_003",
        "question": "Total sales revenue by region",
        "expected_sql": "SELECT c.region, SUM(s.total_amount) as revenue FROM sales s JOIN customers c ON s.customer_id = c.id GROUP BY c.region",
        "generated_sql": "SELECT c.region, SUM(s.total_amount) AS total_revenue FROM sales s INNER JOIN customers c ON s.customer_id = c.id GROUP BY c.region ORDER BY total_revenue DESC",
        "difficulty": "medium",
        "note": "Structural equivalent — different alias, extra ORDER BY, INNER vs implicit JOIN",
    },
    {
        "id": "sim_004",
        "question": "Which department has the highest average salary?",
        "expected_sql": "SELECT d.name, AVG(e.salary) as avg_salary FROM employees e JOIN departments d ON e.department_id = d.id GROUP BY d.name ORDER BY avg_salary DESC LIMIT 1",
        "generated_sql": "SELECT d.name, AVG(e.salary) as avg_salary FROM employees e JOIN departments d ON e.department_id = d.id GROUP BY d.name ORDER BY avg_salary DESC LIMIT 1",
        "difficulty": "medium",
        "note": "Exact match on complex query",
    },
    {
        "id": "sim_005",
        "question": "How many employees are in each department?",
        "expected_sql": "SELECT d.name as department, COUNT(e.id) as employee_count FROM employees e JOIN departments d ON e.department_id = d.id GROUP BY d.name",
        "generated_sql": "SELECT d.name, COUNT(*) as cnt FROM departments d LEFT JOIN employees e ON d.id = e.department_id GROUP BY d.name",
        "difficulty": "medium",
        "note": "Different join direction, COUNT(*) vs COUNT(e.id), different alias",
    },
    {
        "id": "sim_006",
        "question": "Show products with low stock",
        "expected_sql": "SELECT name, stock_quantity FROM products WHERE stock_quantity < 100 ORDER BY stock_quantity ASC",
        "generated_sql": "SELECT name, stock_quantity FROM products WHERE stock_quantity < 50 ORDER BY stock_quantity",
        "difficulty": "easy",
        "note": "Threshold difference — model chose 50 instead of 100",
    },
    {
        "id": "sim_007",
        "question": "What is the total sales revenue?",
        "expected_sql": "SELECT SUM(total_amount) as total_revenue FROM sales",
        "generated_sql": "SELECT SUM(total_amount) as total_revenue FROM sales",
        "difficulty": "easy",
        "note": "Exact match on simple aggregation",
    },
    {
        "id": "sim_008",
        "question": "Find products that have never been sold",
        "expected_sql": "SELECT p.name, p.category, p.price FROM products p LEFT JOIN sales s ON p.id = s.product_id WHERE s.sale_id IS NULL",
        "generated_sql": "SELECT p.name, p.category, p.price FROM products p WHERE p.id NOT IN (SELECT DISTINCT product_id FROM sales)",
        "difficulty": "hard",
        "note": "Different approach (NOT IN subquery vs LEFT JOIN IS NULL) — structurally different but semantically equivalent",
    },
    {
        "id": "sim_009",
        "question": "Show the running total of sales by date",
        "expected_sql": "SELECT sale_date, SUM(total_amount) as daily_total, SUM(SUM(total_amount)) OVER (ORDER BY sale_date) as running_total FROM sales GROUP BY sale_date ORDER BY sale_date",
        "generated_sql": "SELECT sale_date, total_amount, SUM(total_amount) OVER (ORDER BY sale_date ROWS UNBOUNDED PRECEDING) as running_total FROM sales ORDER BY sale_date",
        "difficulty": "hard",
        "note": "Window function variation — different granularity (per-row vs per-day)",
    },
    {
        "id": "sim_010",
        "question": "Compare sales between North America and Europe",
        "expected_sql": "SELECT c.region, COUNT(s.sale_id) as num_sales, SUM(s.total_amount) as total_revenue FROM sales s JOIN customers c ON s.customer_id = c.id WHERE c.region IN ('North America', 'Europe') GROUP BY c.region",
        "generated_sql": "SELECT c.region, COUNT(*) as sale_count, SUM(s.total_amount) as revenue FROM sales s JOIN customers c ON s.customer_id = c.id WHERE c.region IN ('North America', 'Europe') GROUP BY c.region",
        "difficulty": "hard",
        "note": "Minor variation — COUNT(*) vs COUNT(s.sale_id), different alias",
    },
]


def run_offline_eval():
    """Run the offline evaluation and save results."""
    print("=" * 60)
    print("  OFFLINE EVALUATION -- Metrics Validation")
    print("=" * 60)
    print(f"\nRunning {len(SIMULATED_OUTPUTS)} simulated model outputs...\n")

    metrics = EvalMetrics()

    # ── Known schema for hallucination detection ─────────
    known_tables = {"employees", "departments", "products", "customers", "sales"}
    known_columns = {
        "employees": {"id", "name", "salary", "department_id", "role", "hire_date"},
        "departments": {"id", "name", "budget", "location"},
        "products": {"id", "name", "category", "price", "stock_quantity"},
        "customers": {"id", "name", "company", "region", "join_date"},
        "sales": {"sale_id", "employee_id", "customer_id", "product_id", "total_amount", "quantity", "sale_date"},
    }
    halluc_detector = HallucinationDetector(known_tables, known_columns)

    results = []
    exact_matches = 0
    structural_sum = 0.0
    total_hallucinations = 0

    for item in SIMULATED_OUTPUTS:
        # Exact match
        is_exact = metrics.exact_match(item["generated_sql"], item["expected_sql"])
        if is_exact:
            exact_matches += 1

        # Structural similarity
        sim = metrics.structural_similarity(item["generated_sql"], item["expected_sql"])
        structural_sum += sim

        # Hallucination check
        hallucinations = halluc_detector.detect(item["generated_sql"])
        total_hallucinations += len(hallucinations)

        result = {
            "id": item["id"],
            "question": item["question"],
            "expected_sql": item["expected_sql"],
            "generated_sql": item["generated_sql"],
            "exact_match": is_exact,
            "execution_match": is_exact,  # For offline eval, approximate with exact match
            "structural_similarity": sim,
            "hallucinations": hallucinations,
            "difficulty": item["difficulty"],
            "note": item["note"],
        }
        results.append(result)

        status = "[PASS]" if is_exact else ("[WARN]" if sim >= 0.8 else "[FAIL]")
        print(f"  {status} [{item['id']}] {item['question']}")
        print(f"      Exact: {is_exact} | Similarity: {sim:.2f} | Hallucinations: {len(hallucinations)}")
        if not is_exact:
            print(f"      Note: {item['note']}")

    # ── Summary ──────────────────────────────────────────
    total = len(SIMULATED_OUTPUTS)
    summary = {
        "total_queries": total,
        "exact_match_rate": round(exact_matches / total * 100, 1),
        "execution_accuracy": round(exact_matches / total * 100, 1),  # Offline approximation
        "avg_structural_similarity": round(structural_sum / total, 2),
        "total_hallucinations": total_hallucinations,
        "avg_latency_ms": 0,  # Not applicable for offline eval
        "eval_type": "offline_simulated",
        "results": results,
    }

    print(f"\n{'=' * 60}")
    print("  OFFLINE EVALUATION RESULTS")
    print(f"{'=' * 60}")
    print(f"  Total queries:            {total}")
    print(f"  Exact Match Rate:         {summary['exact_match_rate']}%")
    print(f"  Avg Structural Similarity: {summary['avg_structural_similarity']}")
    print(f"  Total Hallucinations:     {summary['total_hallucinations']}")

    # ── Difficulty Breakdown ─────────────────────────────
    print("\n  By Difficulty:")
    for diff in ["easy", "medium", "hard"]:
        diff_results = [r for r in results if r["difficulty"] == diff]
        if diff_results:
            diff_exact = sum(1 for r in diff_results if r["exact_match"])
            diff_sim = sum(r["structural_similarity"] for r in diff_results) / len(diff_results)
            print(f"    {diff:>8}: {diff_exact}/{len(diff_results)} exact match, {diff_sim:.2f} avg similarity")

    # ── Save ─────────────────────────────────────────────
    out_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "baseline_offline.json")
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"\n[SAVED] Results saved to {out_path}")

    return summary


if __name__ == "__main__":
    run_offline_eval()
