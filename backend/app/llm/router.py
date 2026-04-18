"""
Model Router — Routes LLM requests to the optimal provider with fallback.
Supports routing by task type (fast vs accurate), automatic failover,
circuit breaker protection, and retry with exponential backoff.
"""

import time
import threading
import structlog
from typing import Optional

from app.llm.base import BaseLLMProvider
from app.llm.providers import (
    HuggingFaceProvider,
    OpenAIProvider,
    AnthropicProvider,
    OllamaProvider,
)

logger = structlog.get_logger()


class CircuitBreaker:
    """
    Circuit breaker for LLM providers.
    
    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Provider is failing, requests are short-circuited
    - HALF_OPEN: Testing if provider has recovered
    
    Trips after `failure_threshold` consecutive failures.
    Resets after `recovery_timeout` seconds.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

    def __init__(self, failure_threshold: int = 3, recovery_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = self.CLOSED
        self._failure_count = 0
        self._last_failure_time: float = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> str:
        with self._lock:
            if self._state == self.OPEN:
                # Check if recovery timeout has elapsed
                if time.time() - self._last_failure_time >= self.recovery_timeout:
                    self._state = self.HALF_OPEN
            return self._state

    def record_success(self):
        """Record a successful call — resets the breaker."""
        with self._lock:
            self._failure_count = 0
            self._state = self.CLOSED

    def record_failure(self):
        """Record a failed call — may trip the breaker."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            if self._failure_count >= self.failure_threshold:
                self._state = self.OPEN
                logger.warning(
                    "circuit_breaker_opened",
                    failures=self._failure_count,
                    recovery_in_seconds=self.recovery_timeout,
                )

    def is_available(self) -> bool:
        """Check if requests can pass through."""
        return self.state != self.OPEN


class ModelRouter:
    """
    Intelligent model router with fallback chains and circuit breakers.
    
    Routing strategies:
    - "fast": Use the quickest available model (intent classification, simple queries)
    - "accurate": Use the most capable model (complex SQL generation)
    - "default": Use the configured default provider
    """

    def __init__(self, config: dict):
        """
        Initialize with provider configs.
        
        config = {
            "default_provider": "huggingface",
            "huggingface_token": "...",
            "huggingface_model": "Qwen/Qwen2.5-Coder-32B-Instruct",
            "openai_api_key": "...",  # optional
            "anthropic_api_key": "...",  # optional
            "ollama_base_url": "...",  # optional
        }
        """
        self.providers: dict[str, BaseLLMProvider] = {}
        self.breakers: dict[str, CircuitBreaker] = {}
        self.default_provider = config.get("default_provider", "huggingface")
        self._init_providers(config)

        # Routing preferences
        self.routing = {
            "fast": self.default_provider,       # Fast model for classification
            "accurate": self.default_provider,    # Best model for SQL generation
            "default": self.default_provider,     # Default
        }

        # Configure routing based on available providers
        if "openai" in self.providers:
            self.routing["accurate"] = "openai"
        if "anthropic" in self.providers:
            self.routing["accurate"] = "anthropic"

        logger.info(
            "model_router_initialized",
            providers=list(self.providers.keys()),
            default=self.default_provider,
            routing=self.routing,
        )

    def _init_providers(self, config: dict):
        """Initialize available providers based on config."""
        # HuggingFace (primary)
        hf_token = config.get("huggingface_token")
        if hf_token:
            try:
                self.providers["huggingface"] = HuggingFaceProvider(
                    api_token=hf_token,
                    model=config.get("huggingface_model", "Qwen/Qwen2.5-Coder-32B-Instruct"),
                )
                self.breakers["huggingface"] = CircuitBreaker()
                logger.info("provider_initialized", provider="huggingface")
            except Exception as e:
                logger.warning("provider_init_failed", provider="huggingface", error=str(e))

        # OpenAI
        openai_key = config.get("openai_api_key")
        if openai_key:
            try:
                self.providers["openai"] = OpenAIProvider(
                    api_key=openai_key,
                    model=config.get("openai_model", "gpt-4o-mini"),
                )
                self.breakers["openai"] = CircuitBreaker()
                logger.info("provider_initialized", provider="openai")
            except Exception as e:
                logger.warning("provider_init_failed", provider="openai", error=str(e))

        # Anthropic
        anthropic_key = config.get("anthropic_api_key")
        if anthropic_key:
            try:
                self.providers["anthropic"] = AnthropicProvider(
                    api_key=anthropic_key,
                    model=config.get("anthropic_model", "claude-sonnet-4-20250514"),
                )
                self.breakers["anthropic"] = CircuitBreaker()
                logger.info("provider_initialized", provider="anthropic")
            except Exception as e:
                logger.warning("provider_init_failed", provider="anthropic", error=str(e))

        # Ollama (local)
        ollama_url = config.get("ollama_base_url")
        if ollama_url:
            try:
                provider = OllamaProvider(
                    base_url=ollama_url,
                    model=config.get("ollama_model", "llama3"),
                )
                if provider.health_check():
                    self.providers["ollama"] = provider
                    self.breakers["ollama"] = CircuitBreaker()
                    logger.info("provider_initialized", provider="ollama")
                else:
                    logger.warning("provider_unavailable", provider="ollama")
            except Exception as e:
                logger.warning("provider_init_failed", provider="ollama", error=str(e))

        if not self.providers:
            raise RuntimeError("No LLM providers configured. Set at least HUGGINGFACEHUB_API_TOKEN in .env")

    def generate(
        self,
        messages: list[dict],
        model_preference: str = "default",
        max_retries: int = 2,
        timeout: float = 15.0,
        **kwargs,
    ) -> str:
        """
        Route a generation request to the best available provider.
        Falls back through providers if the primary one fails.
        Applies circuit breaker, retry logic, and a total timeout per request.
        """
        # Total deadline prevents thread pool exhaustion under LLM degradation
        deadline = time.monotonic() + timeout

        # Determine target provider
        target = self.routing.get(model_preference, self.default_provider)

        # Build fallback chain: target → default → all others
        fallback_chain = [target]
        if self.default_provider not in fallback_chain:
            fallback_chain.append(self.default_provider)
        for name in self.providers:
            if name not in fallback_chain:
                fallback_chain.append(name)

        # Try each provider in order
        last_error = None
        for provider_name in fallback_chain:
            provider = self.providers.get(provider_name)
            breaker = self.breakers.get(provider_name)
            if not provider:
                continue

            # Abort if total deadline exceeded
            if time.monotonic() > deadline:
                logger.warning("llm_request_timeout", elapsed_providers=len(fallback_chain))
                break

            # Circuit breaker check
            if breaker and not breaker.is_available():
                logger.info("circuit_breaker_skipped", provider=provider_name, state=breaker.state)
                continue

            # Retry loop per provider
            for attempt in range(1, max_retries + 1):
                try:
                    start_time = time.perf_counter()
                    response = provider.generate(messages, **kwargs)
                    elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

                    # Record success
                    if breaker:
                        breaker.record_success()

                    if provider_name != target:
                        logger.info("fallback_used", target=target, actual=provider_name)

                    logger.info(
                        "llm_call_success",
                        provider=provider_name,
                        elapsed_ms=elapsed_ms,
                        attempt=attempt,
                    )
                    return response

                except Exception as e:
                    last_error = e
                    logger.warning(
                        "llm_call_failed",
                        provider=provider_name,
                        attempt=attempt,
                        max_retries=max_retries,
                        error=str(e),
                    )

                    # Don't retry on last attempt — fall through to next provider
                    if attempt == max_retries:
                        if breaker:
                            breaker.record_failure()
                        break

                    # Exponential backoff — but respect the deadline
                    backoff = 0.5 * attempt
                    if time.monotonic() + backoff > deadline:
                        logger.warning("llm_backoff_skipped_deadline", provider=provider_name)
                        if breaker:
                            breaker.record_failure()
                        break
                    time.sleep(backoff)

        raise RuntimeError(f"All LLM providers failed. Last error: {last_error}")

    def get_provider_status(self) -> dict[str, dict]:
        """Check health and circuit breaker state of all registered providers."""
        status = {}
        for name, provider in self.providers.items():
            breaker = self.breakers.get(name)
            status[name] = {
                "healthy": provider.health_check(),
                "circuit_breaker": breaker.state if breaker else "unknown",
            }
        return status

    def list_providers(self) -> list[str]:
        """List all available provider names."""
        return list(self.providers.keys())
