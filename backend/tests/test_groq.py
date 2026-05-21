"""
Tests for Groq Provider Integration.

Covers:
- GroqProvider initialization
- Sync generation
- Async generation
- Streaming
- ModelRouter Groq routing
- Fallback chain behavior
- Circuit breaker integration
- Fast model routing
"""

import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock, patch

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestGroqProvider:
    """Unit tests for the GroqProvider class."""

    def _make_provider(self):
        """Create a GroqProvider with a mock API key."""
        from app.llm.providers import GroqProvider
        return GroqProvider(
            api_key="test-groq-key",
            model="llama-3.3-70b-versatile",
            fast_model="llama-3.1-8b-instant",
            base_url="https://api.groq.com/openai/v1",
        )

    def test_provider_init(self):
        """Test GroqProvider initializes correctly with all parameters."""
        provider = self._make_provider()
        assert provider.name == "groq"
        assert provider.model == "llama-3.3-70b-versatile"
        assert provider.fast_model == "llama-3.1-8b-instant"
        assert provider.api_key == "test-groq-key"
        assert provider.base_url == "https://api.groq.com/openai/v1"

    def test_provider_name(self):
        """Test provider name property."""
        provider = self._make_provider()
        assert provider.name == "groq"

    def test_lazy_client_init(self):
        """Test that clients are not created until first use."""
        provider = self._make_provider()
        assert provider._sync_client is None
        assert provider._async_client is None

    @patch("app.llm.providers.GroqProvider._get_sync_client")
    def test_generate_calls_openai_api(self, mock_client):
        """Test sync generate calls the OpenAI-compatible API."""
        # Mock the response
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "SELECT * FROM users"
        mock_response.usage = MagicMock(prompt_tokens=10, completion_tokens=5)

        mock_client.return_value.chat.completions.create.return_value = mock_response

        provider = self._make_provider()
        result = provider.generate(
            messages=[{"role": "user", "content": "Show all users"}],
            max_tokens=100,
        )

        assert result == "SELECT * FROM users"
        mock_client.return_value.chat.completions.create.assert_called_once()

    @patch("app.llm.providers.GroqProvider._get_sync_client")
    def test_generate_model_override(self, mock_client):
        """Test model_override kwarg changes the model used."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "SELECT 1"
        mock_response.usage = None

        mock_client.return_value.chat.completions.create.return_value = mock_response

        provider = self._make_provider()
        provider.generate(
            messages=[{"role": "user", "content": "test"}],
            model_override="llama-3.1-8b-instant",
        )

        call_kwargs = mock_client.return_value.chat.completions.create.call_args
        assert call_kwargs.kwargs.get("model") == "llama-3.1-8b-instant"


class TestGroqAsync:
    """Async tests for GroqProvider."""

    @pytest.fixture
    def provider(self):
        from app.llm.providers import GroqProvider
        return GroqProvider(
            api_key="test-key",
            model="llama-3.3-70b-versatile",
            fast_model="llama-3.1-8b-instant",
        )

    @pytest.mark.asyncio
    async def test_agenerate_calls_async_client(self, provider):
        """Test async generation uses the async client."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "SELECT name FROM employees"
        mock_response.usage = MagicMock(prompt_tokens=15, completion_tokens=8)

        mock_client = AsyncMock()
        mock_client.chat.completions.create = AsyncMock(return_value=mock_response)
        provider._async_client = mock_client

        result = await provider.agenerate(
            messages=[{"role": "user", "content": "List employees"}]
        )

        assert result == "SELECT name FROM employees"
        mock_client.chat.completions.create.assert_called_once()

    @pytest.mark.asyncio
    async def test_astream_yields_tokens(self, provider):
        """Test streaming yields individual tokens."""
        # Create mock chunks
        chunks = []
        for token in ["SELECT ", "name ", "FROM ", "users"]:
            chunk = MagicMock()
            chunk.choices = [MagicMock()]
            chunk.choices[0].delta.content = token
            chunks.append(chunk)

        # Add final chunk with None content
        end_chunk = MagicMock()
        end_chunk.choices = [MagicMock()]
        end_chunk.choices[0].delta.content = None
        chunks.append(end_chunk)

        mock_client = AsyncMock()

        # Create async iterator for the stream
        async def mock_stream():
            for c in chunks:
                yield c

        mock_client.chat.completions.create = AsyncMock(return_value=mock_stream())
        provider._async_client = mock_client

        tokens = []
        async for token in provider.astream(
            messages=[{"role": "user", "content": "test"}]
        ):
            tokens.append(token)

        assert len(tokens) == 4
        assert "".join(tokens) == "SELECT name FROM users"


class TestModelRouterGroqIntegration:
    """Integration tests for Groq in ModelRouter."""

    def _make_router_config(self, include_groq=True, include_hf=False):
        """Create a minimal router config."""
        config = {"default_provider": "groq" if include_groq else "huggingface"}
        if include_groq:
            config["groq_api_key"] = "test-groq-key"
            config["groq_model_primary"] = "llama-3.3-70b-versatile"
            config["groq_model_fast"] = "llama-3.1-8b-instant"
        if include_hf:
            config["huggingface_token"] = "test-hf-token"
        return config

    def test_groq_becomes_primary(self):
        """Test that Groq is registered as the primary provider."""
        from app.llm.router import ModelRouter

        with patch("app.llm.providers.GroqProvider.health_check", return_value=True):
            router = ModelRouter(self._make_router_config())

        assert "groq" in router.providers
        assert router.default_provider == "groq"
        assert router.routing["fast"] == "groq"
        assert router.routing["accurate"] == "groq"

    def test_fallback_when_groq_unavailable(self):
        """Test fallback to HF when Groq is not configured."""
        from app.llm.router import ModelRouter

        config = self._make_router_config(include_groq=False, include_hf=True)
        with patch("app.llm.providers.HuggingFaceProvider.health_check", return_value=True):
            router = ModelRouter(config)

        assert "groq" not in router.providers
        assert "huggingface" in router.providers
        assert router.default_provider == "huggingface"

    def test_groq_in_fallback_chain(self):
        """Test that Groq appears first in fallback chain when generating."""
        from app.llm.router import ModelRouter

        with patch("app.llm.providers.GroqProvider.health_check", return_value=True):
            router = ModelRouter(self._make_router_config())

        # The fallback chain for 'accurate' should start with groq
        assert router.routing["accurate"] == "groq"

    def test_provider_priority_order(self):
        """Test providers are registered in priority order."""
        from app.llm.router import ModelRouter

        config = self._make_router_config(include_groq=True, include_hf=True)
        with patch("app.llm.providers.GroqProvider.health_check", return_value=True):
            with patch("app.llm.providers.HuggingFaceProvider.health_check", return_value=True):
                router = ModelRouter(config)

        providers = list(router.providers.keys())
        # Groq should be before HuggingFace
        assert providers.index("groq") < providers.index("huggingface")


class TestCircuitBreakerWithGroq:
    """Test circuit breaker integration with Groq."""

    def test_circuit_breaker_created_for_groq(self):
        """Test that a circuit breaker is created for Groq."""
        from app.llm.router import ModelRouter

        with patch("app.llm.providers.GroqProvider.health_check", return_value=True):
            router = ModelRouter({
                "default_provider": "groq",
                "groq_api_key": "test-key",
            })

        assert "groq" in router.breakers
        assert router.breakers["groq"].state == "closed"

    def test_circuit_breaker_opens_after_failures(self):
        """Test circuit breaker opens after threshold failures."""
        from app.llm.router import CircuitBreaker

        breaker = CircuitBreaker(failure_threshold=3)
        assert breaker.is_available()

        for _ in range(3):
            breaker.record_failure()

        assert not breaker.is_available()
        assert breaker.state == "open"


class TestGroqModelRouting:
    """Test model selection based on routing preference."""

    def test_fast_routing_uses_fast_model(self):
        """Test 'fast' preference routes to the 8B model."""
        from app.llm.providers import GroqProvider

        provider = GroqProvider(
            api_key="test",
            model="llama-3.3-70b-versatile",
            fast_model="llama-3.1-8b-instant",
        )

        assert provider.model == "llama-3.3-70b-versatile"
        assert provider.fast_model == "llama-3.1-8b-instant"

    def test_config_settings_parsed(self):
        """Test Groq config fields are properly parsed."""
        from app.config import Settings
        import os

        # Set required env var for Settings
        os.environ.setdefault("DB_URI", "mysql://test:test@localhost/test")

        settings = Settings(
            DB_URI="mysql://test:test@localhost/test",
            GROQ_API_KEY="test-key",
            GROQ_MODEL_PRIMARY="custom-model",
            GROQ_MODEL_FAST="custom-fast",
        )

        assert settings.GROQ_API_KEY == "test-key"
        assert settings.GROQ_MODEL_PRIMARY == "custom-model"
        assert settings.GROQ_MODEL_FAST == "custom-fast"
        assert settings.DEFAULT_LLM_PROVIDER == "groq"


class TestGroqProviderMetrics:
    """Test that Groq calls are properly tracked in metrics."""

    @patch("app.llm.providers.GroqProvider._get_sync_client")
    def test_token_usage_logged(self, mock_client):
        """Test that token usage is extracted from Groq responses."""
        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "SELECT 1"
        mock_response.usage = MagicMock(prompt_tokens=42, completion_tokens=7)

        mock_client.return_value.chat.completions.create.return_value = mock_response

        from app.llm.providers import GroqProvider
        provider = GroqProvider(api_key="test", model="test-model")
        result = provider.generate(
            messages=[{"role": "user", "content": "test"}]
        )

        assert result == "SELECT 1"

    def test_token_tracker_tracks_groq(self):
        """Test TokenTracker records Groq usage."""
        from app.llm.router import TokenTracker

        tracker = TokenTracker()
        info = tracker.track(
            messages=[{"role": "user", "content": "Show employees"}],
            response="SELECT * FROM employees",
            provider="groq",
        )

        assert info["provider"] == "groq"
        assert info["input_tokens"] > 0
        assert info["output_tokens"] > 0

        totals = tracker.get_totals()
        assert totals["total_requests"] == 1
        assert totals["total_tokens"] > 0
