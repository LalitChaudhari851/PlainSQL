"""
Evaluation Pipeline — Measures SQL generation quality with multiple metrics.
Runs the evaluation dataset through the system and reports accuracy.

Metrics:
  - Exact Match       : Normalized SQL string equality
  - Execution Accuracy: Result set comparison (order-independent) — the gold standard
  - Structural Sim    : Clause-level comparison (SELECT/JOIN/WHERE/GROUP BY)
  - Hallucination     : References to non-existent tables/columns

LangSmith Integration:
  When LANGSMITH_API_KEY is set, results are pushed as a structured
  dataset + per-run feedback so you can track accuracy over time and
  compare prompt versions side-by-side in the LangSmith UI.
"""

import json
import os
import sys
import time
import uuid
import sqlparse
import structlog
from typing import Optional

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
        """
        Check if result sets match semantically.
        Normalizations applied:
        1. Ignore column aliases (compare values only)
        2. Ignore column ordering (sort values within each row)
        3. Ignore row ordering (1-to-1 bipartite matching)
        4. Handle SELECT * by checking if expected values are a subset of predicted values
        5. Numeric normalization (float precision, rounding, int-float equivalence)
        6. Null and boolean normalization
        """
        if not predicted_results and not expected_results:
            return True
        if not predicted_results or not expected_results:
            return False

        def normalize_value(val):
            """Normalize value for robust semantic comparison."""
            if val is None:
                return ""
            if isinstance(val, bool):
                return "1" if val else "0"
            
            # Handle Decimals, ints, floats, or numeric strings
            try:
                f_val = float(val)
                rounded = round(f_val, 4)
                if rounded.is_integer():
                    return str(int(rounded))
                return str(rounded)
            except (ValueError, TypeError):
                pass
                
            return str(val).strip().lower()

        def get_normalized_row(row):
            # Extract values, normalize, and sort to ignore column order
            return tuple(sorted(normalize_value(v) for v in row.values()))

        pred_rows = [get_normalized_row(row) for row in predicted_results]
        exp_rows = [get_normalized_row(row) for row in expected_results]

        # 1. Exact match of normalized values (fast path)
        if sorted(pred_rows) == sorted(exp_rows):
            return True
            
        # 2. Subset match (handles SELECT * vs explicit columns and reordered rows)
        if len(pred_rows) == len(exp_rows):
            matched_pred_indices = set()
            all_subset = True
            
            for e_row in exp_rows:
                match_found = False
                for i, p_row in enumerate(pred_rows):
                    if i in matched_pred_indices:
                        continue
                    
                    # Check if e_row is subset of p_row (accounting for duplicates)
                    p_list = list(p_row)
                    is_subset = True
                    for val in e_row:
                        if val in p_list:
                            p_list.remove(val)
                        else:
                            is_subset = False
                            break
                    
                    if is_subset:
                        matched_pred_indices.add(i)
                        match_found = True
                        break
                
                if not match_found:
                    all_subset = False
                    break
                    
            if all_subset:
                return True

        return False

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
    """
    Detects references to non-existent tables/columns in generated SQL.

    Production-grade implementation that correctly handles:
    - SELECT ... AS alias definitions (the #1 source of false positives)
    - FROM/JOIN table aliases (e.g., FROM employees e)
    - CTE names (WITH cte_name AS (...))
    - Comprehensive SQL function and keyword sets
    - Window function keywords (OVER, PARTITION, ROWS, etc.)
    """

    # SQL functions that appear as Name tokens in sqlparse
    SQL_FUNCTIONS = frozenset({
        "count", "sum", "avg", "min", "max", "round", "coalesce",
        "ifnull", "isnull", "nullif", "concat", "substring", "trim",
        "upper", "lower", "length", "cast", "convert", "date_format",
        "date_sub", "date_add", "curdate", "now", "year", "month",
        "day", "hour", "minute", "second", "datediff", "timestampdiff",
        "group_concat", "distinct", "if", "abs", "ceil", "floor",
        "power", "sqrt", "mod", "sign", "truncate", "replace",
        "lpad", "rpad", "left", "right", "reverse", "space",
        "char_length", "character_length", "locate", "instr",
        "field", "elt", "format", "hex", "unhex", "crc32",
        "last_insert_id", "row_number", "rank", "dense_rank",
        "ntile", "lag", "lead", "first_value", "last_value",
        "nth_value", "percent_rank", "cume_dist", "quarter",
        "week", "dayofweek", "dayofyear", "monthname", "dayname",
        "time", "timestamp", "str_to_date", "date",
    })

    # SQL keywords that sqlparse may emit as Name tokens
    SQL_KEYWORDS = frozenset({
        "asc", "desc", "limit", "offset", "as", "on", "and", "or",
        "not", "in", "between", "like", "is", "null", "true", "false",
        "inner", "left", "right", "outer", "cross", "natural",
        "select", "from", "where", "join", "group", "order", "having",
        "by", "union", "except", "intersect", "with", "recursive",
        "case", "when", "then", "else", "end", "exists", "any", "all",
        "interval", "rows", "unbounded", "preceding", "following",
        "over", "partition", "range", "current", "row",
        "rollup", "cube", "grouping", "sets",
    })

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

    def _extract_defined_aliases(self, sql: str) -> set[str]:
        """
        Extract ALL aliases defined in the SQL to prevent false positives.

        Covers three alias sources:
        1. SELECT column aliases:  SUM(x) AS total_revenue, name AS department
        2. FROM/JOIN table aliases: FROM employees e, JOIN departments AS d
        3. CTE names:              WITH top_emp AS (...)
        """
        import re
        aliases = set()

        # 1. SELECT ... AS alias_name  (column aliases)
        for m in re.finditer(r'\bAS\s+`?(\w+)`?', sql, re.IGNORECASE):
            aliases.add(m.group(1).lower())

        # 2. FROM/JOIN table alias  (with or without AS keyword)
        for m in re.finditer(
            r'(?:FROM|JOIN)\s+`?(\w+)`?\s+(?:AS\s+)?`?(\w+)`?',
            sql, re.IGNORECASE,
        ):
            alias = m.group(2).lower()
            table = m.group(1).lower()
            # Only treat as alias if it differs from the table name
            # and isn't a SQL keyword like WHERE, ON, etc.
            if alias != table and alias not in self.SQL_KEYWORDS:
                aliases.add(alias)

        # 3. CTE names: WITH cte_name AS (...)
        for m in re.finditer(r'\bWITH\s+(\w+)\s+AS\s*\(', sql, re.IGNORECASE):
            aliases.add(m.group(1).lower())

        return aliases

    def detect(self, sql: str) -> list[str]:
        """
        Find references to non-existent tables or columns.

        Returns a list of hallucination warnings. Empty list = clean SQL.
        """
        hallucinations = []
        if not sql:
            return hallucinations

        try:
            parsed = sqlparse.parse(sql)
            if not parsed:
                return hallucinations

            # Pre-extract all defined aliases to avoid false positives
            defined_aliases = self._extract_defined_aliases(sql)

            for token in parsed[0].flatten():
                if token.ttype is sqlparse.tokens.Name:
                    name = token.value.lower().strip('`"[]')

                    # Skip SQL functions and keywords
                    if name in self.SQL_FUNCTIONS or name in self.SQL_KEYWORDS:
                        continue

                    # Skip single-char aliases (e.g., e, s, p, c, d)
                    if len(name) <= 1:
                        continue

                    # Skip defined aliases (e.g., revenue, avg_salary, department)
                    if name in defined_aliases:
                        continue

                    # Check against known schema
                    if name not in self.known_tables and name not in self.all_columns:
                        hallucinations.append(f"Unknown identifier: {name}")

        except Exception as e:
            logger.warning("hallucination_detection_failed", error=str(e))

        return hallucinations


class RetrievalMetrics:
    """
    Measures retrieval quality by comparing retrieved schema documents
    against the expected tables for each query.

    Metrics:
      - recall@k:    Fraction of needed tables that were actually retrieved
      - precision@k: Fraction of retrieved tables that were actually needed
    """

    @staticmethod
    def extract_table_names(schema_docs: list[str]) -> set[str]:
        """
        Extract table names from retrieved schema documents.
        Looks for 'Table: <name>' patterns used by the SchemaEnricher.
        """
        import re
        tables = set()
        for doc in schema_docs:
            # Match "Table: employees" or "TABLE employees" patterns
            for m in re.finditer(r'(?:Table|TABLE)[:\s]+`?(\w+)`?', doc, re.IGNORECASE):
                tables.add(m.group(1).lower())
        return tables

    @staticmethod
    def recall_at_k(retrieved_tables: set[str], expected_tables: list[str]) -> float:
        """What fraction of needed tables were retrieved?"""
        if not expected_tables:
            return 1.0
        expected_set = {t.lower() for t in expected_tables}
        return len(retrieved_tables & expected_set) / len(expected_set)

    @staticmethod
    def precision_at_k(retrieved_tables: set[str], expected_tables: list[str]) -> float:
        """What fraction of retrieved tables were actually needed?"""
        if not retrieved_tables:
            return 0.0
        expected_set = {t.lower() for t in expected_tables}
        return len(retrieved_tables & expected_set) / len(retrieved_tables)

class SemanticSQLJudge:
    """
    Uses an LLM to determine if two SQL queries are semantically equivalent.

    Two queries are semantically equivalent if they would return the same result
    set, even if they differ in:
    - Column aliases (AS revenue vs AS total)
    - Column order (SELECT a, b vs SELECT b, a)
    - Equivalent WHERE clauses (price > 50 AND price < 200 vs price BETWEEN 50 AND 200)
    - LIMIT differences (ignored)
    - Whitespace and formatting

    This bridges the gap between exact_match (too strict) and execution_match
    (requires a live database).
    """

    _JUDGE_PROMPT = """You are an expert SQL equivalence judge. Determine if two SQL queries are SEMANTICALLY EQUIVALENT — meaning they would produce the SAME result set on the same database.

IGNORE differences in:
- Column aliases (AS revenue vs AS total_revenue)
- Column ordering (SELECT a, b vs SELECT b, a)  
- LIMIT clauses
- Whitespace and formatting
- Equivalent expressions (BETWEEN vs two comparisons, IN vs multiple OR)

CONSIDER as DIFFERENT:
- Different tables being queried
- Different JOIN conditions
- Different WHERE filters that change results
- Different aggregation logic (GROUP BY on different columns)
- Missing or extra columns in SELECT

Query A (expected):
{expected_sql}

Query B (generated):
{generated_sql}

Respond with ONLY a JSON object:
{{"equivalent": true/false, "reason": "brief explanation"}}"""

    def __init__(self, llm_router=None):
        self._llm_router = llm_router

    @property
    def available(self) -> bool:
        return self._llm_router is not None

    def judge(self, expected_sql: str, generated_sql: str) -> dict:
        """
        Judge semantic equivalence between two SQL queries.

        Returns:
            {"equivalent": bool, "reason": str, "error": optional str}
        """
        if not self.available:
            return {"equivalent": None, "reason": "No LLM available for judging"}

        try:
            prompt = self._JUDGE_PROMPT.format(
                expected_sql=expected_sql.strip(),
                generated_sql=generated_sql.strip(),
            )

            response = self._llm_router.generate(
                messages=[
                    {"role": "system", "content": "You are a precise SQL equivalence judge. Output only JSON."},
                    {"role": "user", "content": prompt},
                ],
                model_preference="fast",
                timeout=10.0,
            )

            # Parse LLM response
            import json
            import re

            # Extract JSON from potential markdown code blocks
            json_match = re.search(r'\{[^{}]*"equivalent"[^{}]*\}', response, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
                return {
                    "equivalent": bool(result.get("equivalent", False)),
                    "reason": result.get("reason", ""),
                }

            return {"equivalent": None, "reason": f"Could not parse LLM response: {response[:100]}"}

        except Exception as e:
            logger.warning("semantic_judge_failed", error=str(e))
            return {"equivalent": None, "reason": f"Judge error: {str(e)[:100]}"}


class LangSmithEvalReporter:
    """
    Pushes evaluation results to LangSmith as a structured dataset.

    For each eval case it creates:
      - A LangSmith run (chain) with inputs + outputs + reference_outputs
      - Four feedback scores: execution_accuracy, exact_match,
        structural_similarity, hallucination_count

    This lets you:
      - Compare prompt versions side-by-side in the LangSmith UI
      - Track accuracy over time across runs
      - Filter failing cases by difficulty or category
    """

    DATASET_NAME = "plainsql-text-to-sql-eval"
    PROJECT_NAME = "plainsql-evaluation"

    def __init__(self, api_key: Optional[str] = None):
        self.enabled = False
        self.client = None

        if not api_key:
            logger.info("langsmith_eval_disabled", reason="no_api_key")
            return

        try:
            from langsmith import Client
            self.client = Client(api_key=api_key)
            self.enabled = True
            logger.info("langsmith_eval_reporter_ready", dataset=self.DATASET_NAME)
        except ImportError:
            logger.warning("langsmith_not_installed", hint="pip install langsmith")
        except Exception as e:
            logger.warning("langsmith_reporter_init_failed", error=str(e))

    def _get_or_create_dataset(self):
        """Get existing dataset or create a new one."""
        try:
            datasets = list(self.client.list_datasets(dataset_name=self.DATASET_NAME))
            if datasets:
                return datasets[0]
            return self.client.create_dataset(
                dataset_name=self.DATASET_NAME,
                description="PlainSQL text-to-SQL evaluation benchmark — 35 cases across easy/medium/hard.",
            )
        except Exception as e:
            logger.warning("langsmith_dataset_fetch_failed", error=str(e))
            return None

    def push(self, results: list[dict], summary: dict) -> bool:
        """
        Push all eval results to LangSmith.
        Returns True if successful, False otherwise.
        """
        if not self.enabled or not self.client:
            return False

        try:
            dataset = self._get_or_create_dataset()
            if not dataset:
                return False

            pushed = 0
            for result in results:
                run_id = str(uuid.uuid4())
                try:
                    # ── Create the eval run ───────────────────────
                    self.client.create_run(
                        id=run_id,
                        name="plainsql_eval_case",
                        run_type="chain",
                        project_name=self.PROJECT_NAME,
                        inputs={"question": result["question"]},
                        outputs={"generated_sql": result["generated_sql"]},
                        reference_outputs={"expected_sql": result["expected_sql"]},
                        extra={
                            "metadata": {
                                "id": result["id"],
                                "difficulty": result["difficulty"],
                                "latency_ms": result["latency_ms"],
                                "retrieval_source": result.get("retrieval_source", "unknown"),
                            }
                        },
                    )

                    # ── Attach per-metric feedback ─────────────────
                    feedback_items = [
                        ("execution_accuracy",    1.0 if result["execution_match"] else 0.0,
                         "Result sets match (order-independent comparison)"),
                        ("exact_match",           1.0 if result["exact_match"] else 0.0,
                         "Normalized SQL string equality"),
                        ("structural_similarity", result["structural_similarity"],
                         "Clause-level match score (SELECT/JOIN/WHERE/GROUP BY)"),
                        ("hallucination_count",   float(len(result["hallucinations"])),
                         "Number of non-existent table/column references"),
                    ]
                    for key, score, comment in feedback_items:
                        self.client.create_feedback(
                            run_id=run_id,
                            key=key,
                            score=score,
                            comment=comment,
                        )

                    pushed += 1

                except Exception as case_err:
                    logger.warning("langsmith_case_push_failed",
                                   case_id=result["id"], error=str(case_err))

            logger.info(
                "langsmith_eval_pushed",
                pushed=pushed,
                total=len(results),
                execution_accuracy=summary["execution_accuracy"],
                dataset=self.DATASET_NAME,
                project=self.PROJECT_NAME,
            )
            return True

        except Exception as e:
            logger.error("langsmith_push_failed", error=str(e))
            return False


class EvalRunner:
    """Runs the full evaluation pipeline."""

    def __init__(self, orchestrator, db_pool, langsmith_api_key: Optional[str] = None):
        self.orchestrator = orchestrator
        self.db_pool = db_pool
        self.metrics = EvalMetrics()
        self.langsmith_reporter = LangSmithEvalReporter(api_key=langsmith_api_key)

        # Semantic SQL judge — uses the LLM to check query equivalence
        llm_router = getattr(orchestrator, 'llm_router', None)
        self.semantic_judge = SemanticSQLJudge(llm_router=llm_router)

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
                import re
                # Remove LIMIT before execution to ignore pagination differences
                def strip_limit(sql: str) -> str:
                    return re.sub(r'(?i)\s+LIMIT\s+\d+(\s*(,|OFFSET)\s*\d+)?\s*;?\s*$', '', str(sql).strip().rstrip(';'))

                exec_pred_sql = strip_limit(generated_sql)
                exec_exp_sql = strip_limit(item["expected_sql"])

                predicted_results = self.db_pool.execute_query(exec_pred_sql)
                expected_results = self.db_pool.execute_query(exec_exp_sql)
                
                is_exec_match = self.metrics.execution_match(predicted_results, expected_results)
                if is_exec_match:
                    execution_matches += 1
            except Exception as e:
                logger.warning("exec_comparison_failed", error=str(e))

            # Hallucination check
            hallucinations = halluc_detector.detect(generated_sql)
            total_hallucinations += len(hallucinations)

            # Retrieval quality check (if expected_tables defined)
            retrieval_recall = None
            retrieval_precision = None
            expected_tables = item.get("expected_tables", [])
            if expected_tables:
                # Extract table names from the retrieved schema context
                retrieved_schema = state.get("relevant_schema", "")
                schema_docs = [retrieved_schema] if isinstance(retrieved_schema, str) else retrieved_schema
                retrieved_tables = RetrievalMetrics.extract_table_names(schema_docs)
                retrieval_recall = round(RetrievalMetrics.recall_at_k(retrieved_tables, expected_tables), 3)
                retrieval_precision = round(RetrievalMetrics.precision_at_k(retrieved_tables, expected_tables), 3)

            # Semantic equivalence check (when not exact match but might be equivalent)
            semantic_equivalent = None
            semantic_reason = ""
            if not is_exact and generated_sql and self.semantic_judge.available:
                judgment = self.semantic_judge.judge(item["expected_sql"], generated_sql)
                semantic_equivalent = judgment.get("equivalent")
                semantic_reason = judgment.get("reason", "")

            result = {
                "id": item["id"],
                "question": item["question"],
                "expected_sql": item["expected_sql"],
                "generated_sql": generated_sql,
                "exact_match": is_exact,
                "execution_match": is_exec_match,
                "semantic_equivalent": semantic_equivalent,
                "semantic_reason": semantic_reason,
                "structural_similarity": structural_sim,
                "hallucinations": hallucinations,
                "retrieval_recall": retrieval_recall,
                "retrieval_precision": retrieval_precision,
                "latency_ms": elapsed,
                "difficulty": item.get("difficulty", "unknown"),
            }
            results.append(result)

            sem_icon = "🟢" if semantic_equivalent else ("" if semantic_equivalent is None else "🔴")
            status = "✅" if is_exec_match else ("⚠️" if is_exact else "❌")
            print(f"   {status} Exact: {is_exact} | Exec: {is_exec_match} | Sim: {structural_sim:.2f} | Sem: {sem_icon} | {elapsed}ms")

        # Summary
        total = len(dataset)

        # Compute retrieval metrics averages (only for items with expected_tables)
        recall_values = [r["retrieval_recall"] for r in results if r["retrieval_recall"] is not None]
        precision_values = [r["retrieval_precision"] for r in results if r["retrieval_precision"] is not None]

        # Compute semantic equivalence rate
        semantic_values = [r["semantic_equivalent"] for r in results if r["semantic_equivalent"] is not None]
        semantic_rate = round(sum(1 for v in semantic_values if v) / len(semantic_values) * 100, 1) if semantic_values else None

        summary = {
            "total_queries": total,
            "exact_match_rate": round(exact_matches / total * 100, 1) if total else 0,
            "execution_accuracy": round(execution_matches / total * 100, 1) if total else 0,
            "semantic_equivalence_rate": semantic_rate,
            "avg_structural_similarity": round(sum(r["structural_similarity"] for r in results) / total, 2) if total else 0,
            "total_hallucinations": total_hallucinations,
            "avg_retrieval_recall": round(sum(recall_values) / len(recall_values), 3) if recall_values else None,
            "avg_retrieval_precision": round(sum(precision_values) / len(precision_values), 3) if precision_values else None,
            "avg_latency_ms": round(total_time / total, 2) if total else 0,
            "results": results,
        }

        print(f"\n{'='*60}")
        print(f"📊 EVALUATION RESULTS")
        print(f"{'='*60}")
        print(f"   Exact Match:         {summary['exact_match_rate']}%")
        print(f"   Execution Accuracy:  {summary['execution_accuracy']}%")
        if semantic_rate is not None:
            print(f"   Semantic Equivalent: {semantic_rate}%")
        print(f"   Avg Similarity:      {summary['avg_structural_similarity']}")
        print(f"   Hallucinations:      {summary['total_hallucinations']}")
        if summary['avg_retrieval_recall'] is not None:
            print(f"   Retrieval Recall:    {summary['avg_retrieval_recall']}")
            print(f"   Retrieval Precision: {summary['avg_retrieval_precision']}")
        print(f"   Avg Latency:         {summary['avg_latency_ms']}ms")

        # ── Push to LangSmith ────────────────────────────
        pushed = self.langsmith_reporter.push(results, summary)
        if pushed:
            print(f"   LangSmith:           ✅ pushed to '{LangSmithEvalReporter.PROJECT_NAME}'")
        elif self.langsmith_reporter.enabled:
            print(f"   LangSmith:           ⚠️  push failed — check logs")
        else:
            print(f"   LangSmith:           — not configured (set LANGSMITH_API_KEY)")

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

    runner = EvalRunner(
        orchestrator=orchestrator,
        db_pool=db_pool,
        langsmith_api_key=settings.LANGSMITH_API_KEY,  # None → local-only mode
    )
    results = runner.run()

    # Save results locally regardless of LangSmith status
    out_path = os.path.join(os.path.dirname(__file__), "results", "latest.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\n💾 Results saved to {out_path}")
