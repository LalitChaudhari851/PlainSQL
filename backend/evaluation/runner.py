"""
Evaluation Pipeline — Measures SQL generation quality with multiple metrics.
Runs the evaluation dataset through the system and reports accuracy.
"""

import json
import os
import sys
import time
import sqlparse
import structlog

# Add project to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = structlog.get_logger()


class EvalMetrics:
    """Evaluation metrics for text-to-SQL accuracy."""

    @staticmethod
    def normalize_sql(sql: str) -> str:
        """Normalize SQL for comparison: uppercase keywords, strip whitespace."""
        formatted = sqlparse.format(
            sql,
            keyword_case="upper",
            strip_comments=True,
            reindent=False,
        )
        # Normalize whitespace
        normalized = " ".join(formatted.split()).strip().rstrip(";")
        return normalized

    @staticmethod
    def exact_match(predicted: str, expected: str) -> bool:
        """Check if normalized SQL matches exactly."""
        return EvalMetrics.normalize_sql(predicted) == EvalMetrics.normalize_sql(expected)

    @staticmethod
    def execution_match(predicted_results: list, expected_results: list) -> bool:
        """Check if result sets match (order-independent)."""
        if not predicted_results and not expected_results:
            return True
        if not predicted_results or not expected_results:
            return False

        # Convert to sorted tuples for comparison
        def to_tuples(results):
            return sorted(tuple(sorted(row.items())) for row in results)

        return to_tuples(predicted_results) == to_tuples(expected_results)

    @staticmethod
    def structural_similarity(predicted: str, expected: str) -> float:
        """
        Calculate structural similarity between two SQL queries.
        Checks for matching clauses (SELECT, FROM, WHERE, JOIN, GROUP BY, ORDER BY).
        Returns a score from 0.0 to 1.0.
        """
        pred_upper = predicted.upper()
        exp_upper = expected.upper()

        clauses = ["SELECT", "FROM", "WHERE", "JOIN", "GROUP BY", "ORDER BY", "HAVING", "LIMIT"]
        matches = 0
        total = 0

        for clause in clauses:
            pred_has = clause in pred_upper
            exp_has = clause in exp_upper
            if pred_has or exp_has:
                total += 1
                if pred_has == exp_has:
                    matches += 1

        return matches / total if total > 0 else 0.0


class HallucinationDetector:
    """Detects references to non-existent tables/columns in generated SQL."""

    def __init__(self, known_tables: set, known_columns: dict):
        """
        known_tables: set of table names
        known_columns: dict of {table_name: set of column_names}
        """
        self.known_tables = {t.lower() for t in known_tables}
        self.known_columns = {
            t.lower(): {c.lower() for c in cols}
            for t, cols in known_columns.items()
        }
        # Flat set of all known column names
        self.all_columns = set()
        for cols in self.known_columns.values():
            self.all_columns.update(cols)

    def detect(self, sql: str) -> list[str]:
        """Find references to non-existent tables or columns."""
        hallucinations = []

        try:
            parsed = sqlparse.parse(sql)
            if not parsed:
                return hallucinations

            # Extract all identifiers
            for token in parsed[0].flatten():
                if token.ttype is sqlparse.tokens.Name:
                    name = token.value.lower()
                    # Skip SQL functions and aliases
                    sql_functions = {"count", "sum", "avg", "min", "max", "date_format", "date_sub", "curdate", "round", "coalesce", "ifnull"}
                    if name in sql_functions:
                        continue
                    # Check if it's a known table or column
                    if name not in self.known_tables and name not in self.all_columns:
                        # Could be an alias — skip short names
                        if len(name) > 1:
                            hallucinations.append(f"Unknown identifier: {name}")

        except Exception as e:
            logger.warning("hallucination_detection_failed", error=str(e))

        return hallucinations


class EvalRunner:
    """Runs the full evaluation pipeline."""

    def __init__(self, orchestrator, db_pool):
        self.orchestrator = orchestrator
        self.db_pool = db_pool
        self.metrics = EvalMetrics()

    def run(self, dataset_path: str = None) -> dict:
        """Run evaluation and return results."""
        if dataset_path is None:
            dataset_path = os.path.join(os.path.dirname(__file__), "dataset.json")

        with open(dataset_path, "r") as f:
            dataset = json.load(f)

        # Build hallucination detector
        tables = self.db_pool.get_tables()
        known_columns = {}
        for table in tables:
            cols = self.db_pool.get_table_schema(table)
            known_columns[table] = [c["name"] for c in cols]
        halluc_detector = HallucinationDetector(set(tables), known_columns)

        results = []
        exact_matches = 0
        execution_matches = 0
        total_hallucinations = 0
        total_time = 0

        for item in dataset:
            print(f"\n📝 Evaluating: {item['question']}")
            start = time.time()

            # Run through pipeline
            state = self.orchestrator.process_query(user_query=item["question"])
            elapsed = round((time.time() - start) * 1000, 2)
            total_time += elapsed

            generated_sql = state.get("sanitized_sql") or state.get("generated_sql", "")

            # Exact match
            is_exact = self.metrics.exact_match(generated_sql, item["expected_sql"])
            if is_exact:
                exact_matches += 1

            # Structural similarity
            structural_sim = self.metrics.structural_similarity(generated_sql, item["expected_sql"])

            # Execution match
            is_exec_match = False
            try:
                predicted_results = self.db_pool.execute_query(generated_sql)
                expected_results = self.db_pool.execute_query(item["expected_sql"])
                is_exec_match = self.metrics.execution_match(predicted_results, expected_results)
                if is_exec_match:
                    execution_matches += 1
            except Exception as e:
                logger.warning("exec_comparison_failed", error=str(e))

            # Hallucination check
            hallucinations = halluc_detector.detect(generated_sql)
            total_hallucinations += len(hallucinations)

            result = {
                "id": item["id"],
                "question": item["question"],
                "expected_sql": item["expected_sql"],
                "generated_sql": generated_sql,
                "exact_match": is_exact,
                "execution_match": is_exec_match,
                "structural_similarity": structural_sim,
                "hallucinations": hallucinations,
                "latency_ms": elapsed,
                "difficulty": item.get("difficulty", "unknown"),
            }
            results.append(result)

            status = "✅" if is_exec_match else ("⚠️" if is_exact else "❌")
            print(f"   {status} Exact: {is_exact} | Exec: {is_exec_match} | Sim: {structural_sim:.2f} | {elapsed}ms")

        # Summary
        total = len(dataset)
        summary = {
            "total_queries": total,
            "exact_match_rate": round(exact_matches / total * 100, 1) if total else 0,
            "execution_accuracy": round(execution_matches / total * 100, 1) if total else 0,
            "avg_structural_similarity": round(sum(r["structural_similarity"] for r in results) / total, 2) if total else 0,
            "total_hallucinations": total_hallucinations,
            "avg_latency_ms": round(total_time / total, 2) if total else 0,
            "results": results,
        }

        print(f"\n{'='*60}")
        print(f"📊 EVALUATION RESULTS")
        print(f"{'='*60}")
        print(f"   Exact Match:         {summary['exact_match_rate']}%")
        print(f"   Execution Accuracy:  {summary['execution_accuracy']}%")
        print(f"   Avg Similarity:      {summary['avg_structural_similarity']}")
        print(f"   Hallucinations:      {summary['total_hallucinations']}")
        print(f"   Avg Latency:         {summary['avg_latency_ms']}ms")

        return summary


if __name__ == "__main__":
    # Standalone evaluation runner
    from dotenv import load_dotenv
    load_dotenv()

    from app.config import get_settings
    from app.db.connection import DatabasePool
    from app.llm.router import ModelRouter
    from app.rag.retriever import HybridRetriever
    from app.agents.orchestrator import AgentOrchestrator

    settings = get_settings()
    db_pool = DatabasePool(settings.DB_URI)
    llm_router = ModelRouter({
        "default_provider": settings.DEFAULT_LLM_PROVIDER,
        "huggingface_token": settings.HUGGINGFACEHUB_API_TOKEN,
        "huggingface_model": settings.DEFAULT_MODEL,
    })
    rag = HybridRetriever(db_pool, settings.CHROMA_PERSIST_DIR)
    orchestrator = AgentOrchestrator(llm_router, rag, db_pool)

    runner = EvalRunner(orchestrator, db_pool)
    results = runner.run()

    # Save results
    with open("evaluation_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print("\n💾 Results saved to evaluation_results.json")
