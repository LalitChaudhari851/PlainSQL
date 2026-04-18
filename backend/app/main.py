"""
PlainSQL Enterprise — FastAPI Application Factory.
Wires all components: agents, LLM router, RAG, auth, observability, and API routes.
Also serves the frontend at / so everything runs from one URL.
"""

import sys
import os
import time
import uuid
import traceback

# Ensure backend is on path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, StreamingResponse
from contextlib import asynccontextmanager
import structlog

from app.config import get_settings
from app.observability.logger import setup_logging

# ── Global state ─────────────────────────────────────────
# NOTE: _app_state is written once at startup and read-only during requests.
# Thread-safe for reads under Python GIL. Do NOT mutate during request handling.
_app_state = {}
START_TIME = time.time()

# Path to the frontend directory
FRONTEND_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "frontend")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application startup and shutdown lifecycle."""
    settings = get_settings()

    # ── Setup Logging ────────────────────────────────────
    setup_logging(
        log_level=settings.LOG_LEVEL,
        json_output=(settings.ENV == "production"),
    )
    logger = structlog.get_logger()
    logger.info("startup_begin", app=settings.APP_NAME, version=settings.APP_VERSION, env=settings.ENV)

    try:
        # ── Database ─────────────────────────────────────
        from app.db.connection import DatabasePool
        logger.info("connecting_database")
        db_pool = DatabasePool(
            settings.DB_URI,
            query_timeout=settings.DB_QUERY_TIMEOUT,
            pool_size=settings.DB_POOL_SIZE,
            max_overflow=settings.DB_MAX_OVERFLOW,
            pool_timeout=settings.DB_POOL_TIMEOUT,
        )
        _app_state["db_pool"] = db_pool
        logger.info("database_connected", tables=len(db_pool.get_tables()))

        # ── LLM Router ───────────────────────────────────
        from app.llm.router import ModelRouter
        logger.info("initializing_llm_router")
        llm_config = {
            "default_provider": settings.DEFAULT_LLM_PROVIDER,
            "huggingface_token": settings.HUGGINGFACEHUB_API_TOKEN,
            "huggingface_model": settings.DEFAULT_MODEL,
            "openai_api_key": settings.OPENAI_API_KEY,
            "anthropic_api_key": settings.ANTHROPIC_API_KEY,
            "ollama_base_url": settings.OLLAMA_BASE_URL,
        }
        llm_router = ModelRouter(llm_config)
        _app_state["llm_router"] = llm_router

        # ── RAG Retriever ────────────────────────────────
        from app.rag.retriever import HybridRetriever
        logger.info("initializing_rag")
        rag_retriever = HybridRetriever(db_pool, chroma_persist_dir=settings.CHROMA_PERSIST_DIR)
        _app_state["rag_retriever"] = rag_retriever

        # ── Agent Orchestrator ───────────────────────────
        from app.agents.orchestrator import AgentOrchestrator
        logger.info("building_agent_graph")
        orchestrator = AgentOrchestrator(llm_router, rag_retriever, db_pool)
        _app_state["orchestrator"] = orchestrator

        # ── Auto-migrate persistence tables ────────────
        _ensure_feedback_table(db_pool)
        from app.db.persistence import ensure_tables, ConversationManager
        ensure_tables(db_pool)
        conversation_manager = ConversationManager(db_pool)
        _app_state["conversation_manager"] = conversation_manager

        # ── Auth Service ─────────────────────────────────
        from app.auth.jwt_auth import AuthService
        auth_service = AuthService(
            secret_key=settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM,
            expiry_hours=settings.JWT_EXPIRY_HOURS,
        )
        _app_state["auth_service"] = auth_service

        # ── Create default users ─────────────────────────
        # WARNING: In-memory user store — for demo/portfolio only.
        # Production: migrate to a MySQL `users` table.
        # Registered users are lost on every server restart.
        admin_password = os.environ.get("ADMIN_DEFAULT_PASSWORD", "admin123")
        analyst_password = os.environ.get("ANALYST_DEFAULT_PASSWORD", "analyst123")

        if settings.ENV == "production" and admin_password == "admin123":
            raise ValueError(
                "ADMIN_DEFAULT_PASSWORD must be changed in production. "
                "Set the ADMIN_DEFAULT_PASSWORD environment variable."
            )

        user_store = {
            "admin": {
                "id": "user_1", "username": "admin", "email": "admin@plainsql.io",
                "password_hash": auth_service.hash_password(admin_password),
                "role": "admin", "tenant_id": "default",
            },
            "analyst": {
                "id": "user_2", "username": "analyst", "email": "analyst@plainsql.io",
                "password_hash": auth_service.hash_password(analyst_password),
                "role": "analyst", "tenant_id": "default",
            },
        }
        _app_state["user_store"] = user_store

        # ── Observability ────────────────────────────────
        from app.observability.tracing import QueryTracer
        tracer = QueryTracer(langsmith_api_key=settings.LANGSMITH_API_KEY, project=settings.LANGSMITH_PROJECT)
        _app_state["tracer"] = tracer

        # ── AI Features ──────────────────────────────────
        from app.ai_features.explainer import SQLExplainer
        from app.ai_features.insights import InsightsGenerator
        from app.ai_features.anomaly import AnomalyDetector
        _app_state["explainer"] = SQLExplainer(llm_router)
        _app_state["insights_gen"] = InsightsGenerator()
        _app_state["anomaly_detector"] = AnomalyDetector()

        # ── Input Validator ──────────────────────────────
        from app.security.input_validator import InputValidator
        _app_state["input_validator"] = InputValidator(max_length=1000)

        # ── Cache & Rate Limiting (Redis with in-memory fallback) ──
        from app.cache.redis_client import create_cache, create_rate_limiter
        from app.api.middleware import create_auth_dependency
        _app_state["rate_limiter"] = create_rate_limiter(
            redis_url=settings.REDIS_URL, rpm=settings.RATE_LIMIT_RPM,
        )
        _app_state["cache"] = create_cache(
            redis_url=settings.REDIS_URL, ttl_seconds=settings.CACHE_TTL_SECONDS,
        )
        _app_state["auth_dep"] = create_auth_dependency(auth_service)

        # ── Register API Routes ──────────────────────────
        from app.api.routes.chat import create_chat_router
        from app.api.routes.system import create_system_router

        chat_router = create_chat_router(
            orchestrator=orchestrator, auth_dep=_app_state["auth_dep"],
            cache=_app_state["cache"], rate_limiter=_app_state["rate_limiter"],
            tracer=tracer, explainer=_app_state["explainer"],
            insights_gen=_app_state["insights_gen"], anomaly_detector=_app_state["anomaly_detector"],
            safety_validator=_app_state["input_validator"],
        )
        app.include_router(chat_router)

        auth_router, schema_router, analytics_router, health_router = create_system_router(
            auth_service=auth_service, auth_dep=_app_state["auth_dep"],
            db_pool=db_pool, rag_retriever=rag_retriever, llm_router=llm_router,
            tracer=tracer, user_store=user_store, start_time=START_TIME,
        )
        app.include_router(auth_router)
        app.include_router(schema_router)
        app.include_router(analytics_router)
        app.include_router(health_router)

        # ── Monitoring ───────────────────────────────────
        from app.api.routes.monitoring import create_monitoring_router, get_metrics_collector
        monitoring_router = create_monitoring_router()
        app.include_router(monitoring_router)
        _app_state["metrics_collector"] = get_metrics_collector()

        # ── Conversations API ─────────────────────────────
        from app.api.routes.conversations import create_conversations_router
        conv_router = create_conversations_router(conversation_manager)
        app.include_router(conv_router)

        # ── Request Deduplicator ──────────────────────────
        from app.security.dedup import RequestDeduplicator
        _app_state["dedup"] = RequestDeduplicator()

        # ── Legacy /chat endpoint ────────────────────────
        _register_legacy_chat(app, orchestrator, tracer, _app_state["rate_limiter"], _app_state["input_validator"], _app_state["metrics_collector"], conversation_manager, _app_state["dedup"])

        logger.info("startup_complete",
            providers=llm_router.list_providers(),
            tables=db_pool.get_tables(),
            rag_docs=rag_retriever.collection.count(),
        )
        yield

    except Exception as e:
        logger.error("startup_failed", error=str(e))
        raise
    finally:
        logger.info("shutdown_complete")


def _ensure_feedback_table(db_pool):
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
        structlog.get_logger().info("feedback_table_ready")
    except Exception as e:
        structlog.get_logger().warning("feedback_table_migration_failed", error=str(e))


def _register_legacy_chat(app: FastAPI, orchestrator, tracer, rate_limiter, input_validator, metrics_collector, conversation_manager=None, dedup=None):
    """Backward-compatible /chat endpoint for the frontend — now async with metrics."""
    from pydantic import BaseModel, Field
    from typing import List, Optional
    import json as json_mod

    class LegacyChatRequest(BaseModel):
        question: str = Field(..., min_length=1, max_length=1000)
        history: Optional[List[dict]] = []
        conversation_id: Optional[str] = None

    class FeedbackRequest(BaseModel):
        message_id: str = Field(..., min_length=1, max_length=64)
        user_query: str = Field(..., min_length=1, max_length=1000)
        generated_sql: Optional[str] = ""
        rating: str = Field(..., pattern="^(up|down)$")
        comment: Optional[str] = ""

    @app.post("/api/v1/feedback")
    async def submit_feedback(request: FeedbackRequest):
        """Store user feedback on generated SQL for RLHF data collection."""
        try:
            db_pool = _app_state.get("db_pool")
            if not db_pool:
                return JSONResponse(status_code=503, content={"error": "Database unavailable"})
            db_pool._execute_write_internal(
                """INSERT INTO query_feedback (message_id, user_query, generated_sql, rating, comment)
                   VALUES (:p0, :p1, :p2, :p3, :p4)""",
                (request.message_id, request.user_query, request.generated_sql or "", request.rating, request.comment or ""),
            )
            structlog.get_logger().info(
                "feedback_recorded",
                message_id=request.message_id,
                rating=request.rating,
            )
            return {"status": "ok", "message": "Feedback recorded. Thank you!"}
        except Exception as e:
            structlog.get_logger().error("feedback_failed", error=str(e))
            return JSONResponse(status_code=500, content={"error": "Failed to save feedback"})

    @app.post("/chat")
    async def legacy_chat(request: LegacyChatRequest, req: Request):
        # ── Authentication (optional JWT — frontend may not send token) ──
        auth_header = req.headers.get("Authorization", "")
        if auth_header.startswith("Bearer "):
            try:
                auth_service = _app_state.get("auth_service")
                if auth_service:
                    auth_service.verify_token(auth_header[7:])
            except Exception:
                return JSONResponse(
                    status_code=401,
                    content={"error": "Invalid or expired authentication token."},
                )

        # Rate limiting by IP
        client_ip = req.client.host if req.client else "unknown"
        if not rate_limiter.check(f"legacy:{client_ip}"):
            return JSONResponse(
                status_code=429,
                content={"error": "Rate limit exceeded. Please wait a moment."},
            )

        # Input validation
        is_safe, rejection_reason, sanitized = input_validator.validate(request.question)
        if not is_safe:
            return JSONResponse(
                status_code=400,
                content={"error": f"Query blocked: {rejection_reason}"},
            )

        result = await orchestrator.aprocess_query(
            user_query=sanitized,
            conversation_history=input_validator.sanitize_history(request.history or []),
        )
        tracer.trace_query(result)

        # Record metrics
        metrics_collector.record_query(
            latency_ms=result.get("execution_time_ms", 0),
            intent=result.get("intent", "unknown"),
            success=not bool(result.get("error")),
            error_agent=result.get("error_agent"),
        )

        return {
            "answer": result.get("query_results", []),
            "sql": result.get("sanitized_sql") or result.get("generated_sql", ""),
            "explanation": result.get("sql_explanation", ""),
            "message": result.get("friendly_message", ""),
            "follow_ups": result.get("follow_up_questions", []),
            "insights": result.get("insights", []),
            "intent": result.get("intent", ""),
            "execution_time_ms": result.get("execution_time_ms", 0),
            "row_count": result.get("row_count", 0),
            "chart_config": result.get("chart_config"),
        }

    @app.post("/chat/stream")
    async def legacy_chat_stream(request: LegacyChatRequest, req: Request):
        """SSE streaming endpoint for the frontend."""
        client_ip = req.client.host if req.client else "unknown"
        if not rate_limiter.check(f"stream:{client_ip}"):
            return JSONResponse(
                status_code=429,
                content={"error": "Rate limit exceeded."},
            )

        is_safe, rejection_reason, sanitized = input_validator.validate(request.question)
        if not is_safe:
            return JSONResponse(
                status_code=400,
                content={"error": f"Query blocked: {rejection_reason}"},
            )

        # ── Request deduplication ─────────────────────────
        is_new_request = True
        query_hash = ""
        if dedup:
            is_new_request, query_hash = dedup.try_acquire(sanitized)
            if not is_new_request:
                # Another request is already processing this query — wait for it
                dedup_result = dedup.wait_for_result(query_hash)
                if dedup_result:
                    async def dedup_generator():
                        yield f"data: {json_mod.dumps({'type': 'stage', 'stage': 'dedup', 'message': 'Using result from concurrent request...'})}\n\n"
                        yield f"data: {json_mod.dumps(dedup_result, default=str)}\n\n"
                        yield f"data: {json_mod.dumps({'type': 'done', 'total_time_ms': 0, 'deduplicated': True})}\n\n"
                    return StreamingResponse(
                        dedup_generator(),
                        media_type="text/event-stream",
                        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
                    )

        # ── Redis cache check ─────────────────────────────
        cache = _app_state.get("cache")
        cached_result = None
        if cache:
            try:
                cached_result = cache.get(sanitized)
            except Exception:
                pass

        # ── Sanitize conversation history ─────────────────
        safe_history = input_validator.sanitize_history(request.history or [])

        async def event_generator():
            import time as time_mod
            start = time_mod.perf_counter()

            # ── Cache HIT: return immediately ─────────────
            if cached_result:
                elapsed_ms = round((time_mod.perf_counter() - start) * 1000, 2)
                yield f"data: {json_mod.dumps({'type': 'stage', 'stage': 'cache_hit', 'message': 'Retrieved from cache...'})}\n\n"
                yield f"data: {json_mod.dumps({'type': 'intent', 'intent': cached_result.get('intent', ''), 'complexity': cached_result.get('complexity', '')}, default=str)}\n\n"
                sql = cached_result.get('sql', '')
                if sql:
                    yield f"data: {json_mod.dumps({'type': 'sql', 'sql': sql, 'explanation': cached_result.get('explanation', '')}, default=str)}\n\n"
                yield f"data: {json_mod.dumps({'type': 'results', 'data': cached_result.get('answer', [])[:100], 'row_count': cached_result.get('row_count', 0), 'execution_time_ms': elapsed_ms}, default=str)}\n\n"
                yield f"data: {json_mod.dumps({'type': 'message', 'message': cached_result.get('message', ''), 'insights': cached_result.get('insights', []), 'follow_ups': cached_result.get('follow_ups', [])}, default=str)}\n\n"
                yield f"data: {json_mod.dumps({'type': 'done', 'total_time_ms': elapsed_ms, 'cached': True})}\n\n"
                structlog.get_logger().info("cache_hit_served", query=sanitized[:50], elapsed_ms=elapsed_ms)
                # Release dedup slot
                if dedup and query_hash:
                    dedup.release(query_hash)
                return

            # ── Cache MISS: run full pipeline ─────────────
            yield f"data: {json_mod.dumps({'type': 'stage', 'stage': 'processing', 'message': 'Analyzing your question...'})}\n\n"

            result = await orchestrator.aprocess_query(
                user_query=sanitized,
                conversation_history=safe_history,
            )

            elapsed_ms = round((time_mod.perf_counter() - start) * 1000, 2)
            tracer.trace_query(result)

            # Record metrics
            metrics_collector.record_query(
                latency_ms=elapsed_ms,
                intent=result.get("intent", "unknown"),
                success=not bool(result.get("error")),
                error_agent=result.get("error_agent"),
            )

            # Stream intent
            yield f"data: {json_mod.dumps({'type': 'intent', 'intent': result.get('intent', ''), 'complexity': result.get('complexity', '')}, default=str)}\n\n"

            # Stream SQL
            sql = result.get("sanitized_sql") or result.get("generated_sql", "")
            if sql:
                yield f"data: {json_mod.dumps({'type': 'sql', 'sql': sql, 'explanation': result.get('sql_explanation', '')}, default=str)}\n\n"

            # Stream results
            yield f"data: {json_mod.dumps({'type': 'results', 'data': result.get('query_results', [])[:100], 'row_count': result.get('row_count', 0), 'execution_time_ms': result.get('execution_time_ms', 0)}, default=str)}\n\n"

            # Stream message + insights
            yield f"data: {json_mod.dumps({'type': 'message', 'message': result.get('friendly_message', ''), 'insights': result.get('insights', []), 'follow_ups': result.get('follow_up_questions', [])}, default=str)}\n\n"

            # Done
            yield f"data: {json_mod.dumps({'type': 'done', 'total_time_ms': elapsed_ms})}\n\n"

            # ── Write to Redis cache (only for successful SQL queries) ──
            if cache and sql and not result.get("error"):
                try:
                    cache_payload = {
                        "sql": sql,
                        "explanation": result.get("sql_explanation", ""),
                        "message": result.get("friendly_message", ""),
                        "answer": result.get("query_results", [])[:100],
                        "intent": result.get("intent", ""),
                        "complexity": result.get("complexity", ""),
                        "row_count": result.get("row_count", 0),
                        "insights": result.get("insights", []),
                        "follow_ups": result.get("follow_up_questions", []),
                    }
                    cache.set(sanitized, cache_payload)
                    structlog.get_logger().info("cache_written", query=sanitized[:50])
                except Exception as cache_err:
                    structlog.get_logger().warning("cache_write_failed", error=str(cache_err))

            # ── Persist messages to MySQL (fire-and-forget) ───────
            if conversation_manager and request.conversation_id:
                try:
                    conversation_manager.save_user_message(request.conversation_id, sanitized)
                    conversation_manager.save_assistant_message(
                        conversation_id=request.conversation_id,
                        content=result.get('friendly_message', ''),
                        generated_sql=sql,
                        explanation=result.get('sql_explanation', ''),
                        friendly_message=result.get('friendly_message', ''),
                        intent=result.get('intent', ''),
                        execution_time_ms=elapsed_ms,
                        row_count=result.get('row_count', 0),
                        result_data=result.get('query_results', []),
                    )
                except Exception as persist_err:
                    structlog.get_logger().warning("message_persist_failed", error=str(persist_err))

            # ── Complete dedup slot (releases waiting clients) ──
            if dedup and query_hash:
                dedup.complete(query_hash, {'type': 'results', 'sql': sql, 'row_count': result.get('row_count', 0)})

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
        )


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="PlainSQL Enterprise API",
        description="Production-grade Text-to-SQL multi-agent system",
        version=settings.APP_VERSION,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ── CORS — use configured origins, not wildcard ──────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["Authorization", "Content-Type", "X-API-Key", "X-Request-ID"],
    )

    # ── Global Exception Handler ─────────────────────────
    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        """
        Catch-all exception handler. Returns structured JSON errors
        without leaking stack traces to clients.
        """
        request_id = getattr(request.state, "request_id", "unknown")
        logger = structlog.get_logger()
        logger.error(
            "unhandled_exception",
            request_id=request_id,
            path=request.url.path,
            method=request.method,
            error_type=type(exc).__name__,
            error=str(exc),
            traceback=traceback.format_exc(),
        )
        return JSONResponse(
            status_code=500,
            content={
                "error": "Internal server error",
                "request_id": request_id,
                "message": "An unexpected error occurred. Please try again or contact support.",
            },
        )

    # ── Request ID Middleware ────────────────────────────
    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        """Assign a unique request ID to every request for correlation."""
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4())[:8])
        request.state.request_id = request_id

        # Bind to structlog context for all log entries in this request
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)

        start_time = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

        response.headers["X-Request-ID"] = request_id
        response.headers["X-Response-Time-Ms"] = str(elapsed_ms)

        structlog.get_logger().info(
            "http_request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            elapsed_ms=elapsed_ms,
        )
        return response

    # ── Serve frontend at root ───────────────────────────
    @app.get("/", response_class=HTMLResponse)
    async def serve_frontend():
        index_path = os.path.join(FRONTEND_DIR, "index.html")
        if os.path.exists(index_path):
            with open(index_path, "r", encoding="utf-8") as f:
                return HTMLResponse(content=f.read())
        return HTMLResponse("<h1>Frontend not found</h1>", status_code=404)

    @app.get("/styles.css")
    async def serve_styles():
        return FileResponse(os.path.join(FRONTEND_DIR, "styles.css"), media_type="text/css")

    @app.get("/app.js")
    async def serve_app_js():
        return FileResponse(os.path.join(FRONTEND_DIR, "app.js"), media_type="application/javascript")

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
