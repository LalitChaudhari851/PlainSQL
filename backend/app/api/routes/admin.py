"""
Admin Routes — Administrative operations requiring admin role.
Includes schema reindexing, cache invalidation, token usage, and system management.
"""

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
import structlog

logger = structlog.get_logger()


def create_admin_router(
    rag_retriever,
    cache,
    auth_dep,
    db_pool,
    llm_router=None,
    orchestrator=None,
):
    """Factory to create admin routes with injected dependencies."""

    router = APIRouter(prefix="/api/v1/admin", tags=["Admin"])

    def _require_admin(user=Depends(auth_dep)):
        """Ensure the authenticated user has admin role."""
        if user.get("role") != "admin":
            raise HTTPException(status_code=403, detail="Admin role required")
        return user

    @router.post("/reindex")
    def reindex_schema(user=Depends(_require_admin)):
        """
        Re-index the database schema into RAG (ChromaDB + BM25).
        Call this after schema changes (new tables, columns, etc.).
        """
        try:
            rag_retriever.refresh_index()
            doc_count = rag_retriever.collection.count()
            logger.info("admin_reindex_complete", triggered_by=user.get("username"), docs=doc_count)
            return {
                "status": "ok",
                "message": f"Schema re-indexed successfully. {doc_count} documents indexed.",
                "document_count": doc_count,
            }
        except Exception as e:
            logger.error("admin_reindex_failed", error=str(e))
            return JSONResponse(
                status_code=500,
                content={"error": f"Reindex failed: {str(e)}"},
            )

    @router.post("/cache/invalidate")
    def invalidate_cache(user=Depends(_require_admin)):
        """Clear the entire query cache."""
        try:
            if hasattr(cache, "invalidate_all"):
                cache.invalidate_all()
            # Also clear semantic cache if available
            if orchestrator and hasattr(orchestrator, "semantic_cache") and orchestrator.semantic_cache:
                orchestrator.semantic_cache.invalidate_all()
            logger.info("admin_cache_invalidated", triggered_by=user.get("username"))
            return {"status": "ok", "message": "Cache invalidated successfully."}
        except Exception as e:
            logger.error("admin_cache_invalidate_failed", error=str(e))
            return JSONResponse(
                status_code=500,
                content={"error": f"Cache invalidation failed: {str(e)}"},
            )

    @router.get("/pool-status")
    def pool_status(user=Depends(_require_admin)):
        """Get database connection pool statistics."""
        try:
            pool_info = db_pool.get_pool_status()
            return {"status": "ok", "pool": pool_info}
        except Exception as e:
            return JSONResponse(
                status_code=500,
                content={"error": f"Pool status unavailable: {str(e)}"},
            )

    @router.get("/token-usage")
    def token_usage(user=Depends(_require_admin)):
        """
        Get aggregate LLM token usage and cost statistics.

        Returns:
            Total input/output tokens, estimated cost, request count,
            and average tokens per request.
        """
        if not llm_router:
            return JSONResponse(
                status_code=503,
                content={"error": "LLM router not available"},
            )
        try:
            usage = llm_router.get_token_usage()
            # Add semantic cache stats if available
            cache_stats = None
            if orchestrator and hasattr(orchestrator, "semantic_cache") and orchestrator.semantic_cache:
                cache_stats = orchestrator.semantic_cache.stats()

            return {
                "status": "ok",
                "token_usage": usage,
                "semantic_cache": cache_stats,
            }
        except Exception as e:
            return JSONResponse(
                status_code=500,
                content={"error": f"Token usage unavailable: {str(e)}"},
            )

    return router

