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
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from contextlib import asynccontextmanager
import structlog

from app.config import get_settings
from app.observability.logger import setup_logging

# ── Global state ─────────────────────────────────────────
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

        # ── Auth Service ─────────────────────────────────
        from app.auth.jwt_auth import AuthService
        auth_service = AuthService(
            secret_key=settings.JWT_SECRET_KEY,
            algorithm=settings.JWT_ALGORITHM,
            expiry_hours=settings.JWT_EXPIRY_HOURS,
        )
        _app_state["auth_service"] = auth_service

        # ── Create default users ─────────────────────────
        user_store = {
            "admin": {
                "id": "user_1", "username": "admin", "email": "admin@plainsql.io",
                "password_hash": auth_service.hash_password("admin123"),
                "role": "admin", "tenant_id": "default",
            },
            "analyst": {
                "id": "user_2", "username": "analyst", "email": "analyst@plainsql.io",
                "password_hash": auth_service.hash_password("analyst123"),
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

        # ── Middleware Components ─────────────────────────
        from app.api.middleware import InMemoryRateLimiter, QueryCache, create_auth_dependency
        _app_state["rate_limiter"] = InMemoryRateLimiter(requests_per_minute=settings.RATE_LIMIT_RPM)
        _app_state["cache"] = QueryCache(ttl_seconds=settings.CACHE_TTL_SECONDS)
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

        # ── Legacy /chat endpoint ────────────────────────
        _register_legacy_chat(app, orchestrator, tracer, _app_state["rate_limiter"], _app_state["input_validator"])

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


def _register_legacy_chat(app: FastAPI, orchestrator, tracer, rate_limiter, input_validator):
    """Backward-compatible /chat endpoint for the frontend — now with input validation and rate limiting."""
    from pydantic import BaseModel, Field
    from typing import List, Optional

    class LegacyChatRequest(BaseModel):
        question: str = Field(..., min_length=1, max_length=1000)
        history: Optional[List[dict]] = []

    @app.post("/chat")
    def legacy_chat(request: LegacyChatRequest, req: Request):
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

        result = orchestrator.process_query(
            user_query=sanitized,
            conversation_history=request.history or [],
        )
        tracer.trace_query(result)
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
