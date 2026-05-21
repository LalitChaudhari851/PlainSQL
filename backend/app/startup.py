"""
Startup Utilities — Database migrations and health checks that run during boot.

Extracted from main.py for maintainability. These run once during app startup
and are not invoked during request handling.
"""

import structlog

logger = structlog.get_logger()


def run_smoke_test(db_pool, rag_retriever, llm_router):
    """
    Startup health check — validates critical components before accepting traffic.
    Logs warnings for degraded components but does NOT crash the server.
    """
    checks = {"database": False, "rag_index": False, "llm_provider": False}

    # 1. Database connectivity
    try:
        result = db_pool.execute_query("SELECT 1 AS health")
        checks["database"] = bool(result)
    except Exception as e:
        logger.error("smoke_test_db_failed", error=str(e))

    # 2. RAG index has documents
    try:
        doc_count = rag_retriever.collection.count()
        checks["rag_index"] = doc_count > 0
        if not checks["rag_index"]:
            logger.warning("smoke_test_rag_empty", doc_count=0)
    except Exception as e:
        logger.error("smoke_test_rag_failed", error=str(e))

    # 3. LLM provider availability
    try:
        providers = llm_router.list_providers()
        checks["llm_provider"] = len(providers) > 0
        if not checks["llm_provider"]:
            logger.error("smoke_test_no_llm_providers")
    except Exception as e:
        logger.error("smoke_test_llm_failed", error=str(e))

    passed = sum(checks.values())
    total = len(checks)
    status = "PASS" if passed == total else "DEGRADED"

    logger.info("smoke_test_complete", status=status, passed=passed, total=total, checks=checks)


def ensure_feedback_table(db_pool):
    """Auto-create the query_feedback table for RLHF data collection."""
    try:
        db_pool._execute_write_internal("""
            CREATE TABLE IF NOT EXISTS query_feedback (
                id INT AUTO_INCREMENT PRIMARY KEY,
                message_id VARCHAR(64) NOT NULL,
                user_query TEXT NOT NULL,
                generated_sql TEXT,
                rating ENUM('up', 'down') NOT NULL,
                comment TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX idx_rating (rating),
                INDEX idx_created (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """)
        logger.info("feedback_table_ready")
    except Exception as e:
        logger.warning("feedback_table_migration_failed", error=str(e))
