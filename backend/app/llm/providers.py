"""
LLM Providers — Concrete implementations for each supported model provider.
"""

import structlog
from huggingface_hub import InferenceClient

from app.llm.base import BaseLLMProvider

logger = structlog.get_logger()


class HuggingFaceProvider(BaseLLMProvider):
    """HuggingFace Inference API provider (Qwen, Mistral, etc.)"""

    def __init__(self, api_token: str, model: str = "Qwen/Qwen2.5-Coder-32B-Instruct"):
        self.api_token = api_token
        self.model = model
        self.client = InferenceClient(token=api_token, timeout=30.0)

    def generate(self, messages: list[dict], **kwargs) -> str:
        max_tokens = kwargs.get("max_tokens", 1024)
        temperature = kwargs.get("temperature", 0.1)

        response = self.client.chat_completion(
            messages=messages,
            model=self.model,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return response.choices[0].message.content

    def health_check(self) -> bool:
        try:
            self.client.chat_completion(
                messages=[{"role": "user", "content": "ping"}],
                model=self.model,
                max_tokens=5,
            )
            return True
        except Exception:
            return False

    @property
    def name(self) -> str:
        return "huggingface"


class OpenAIProvider(BaseLLMProvider):
    """OpenAI API provider (GPT-4, GPT-3.5, etc.)"""

    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.api_key = api_key
        self.model = model
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                from openai import OpenAI
                self._client = OpenAI(api_key=self.api_key)
            except ImportError:
                raise ImportError("openai package not installed. Run: pip install openai")
        return self._client

    def generate(self, messages: list[dict], **kwargs) -> str:
        client = self._get_client()
        max_tokens = kwargs.get("max_tokens", 1024)
        temperature = kwargs.get("temperature", 0.1)

        response = client.chat.completions.create(
            model=self.model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return response.choices[0].message.content

    def health_check(self) -> bool:
        try:
            client = self._get_client()
            client.models.list()
            return True
        except Exception:
            return False

    @property
    def name(self) -> str:
        return "openai"


class AnthropicProvider(BaseLLMProvider):
    """Anthropic API provider (Claude models)"""

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        self.api_key = api_key
        self.model = model
        self._client = None

    def _get_client(self):
        if self._client is None:
            try:
                import anthropic
                self._client = anthropic.Anthropic(api_key=self.api_key)
            except ImportError:
                raise ImportError("anthropic package not installed. Run: pip install anthropic")
        return self._client

    def generate(self, messages: list[dict], **kwargs) -> str:
        client = self._get_client()
        max_tokens = kwargs.get("max_tokens", 1024)

        # Anthropic format: separate system from user messages
        system_msg = ""
        user_messages = []
        for msg in messages:
            if msg["role"] == "system":
                system_msg = msg["content"]
            else:
                user_messages.append(msg)

        response = client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            system=system_msg,
            messages=user_messages,
        )
        return response.content[0].text

    def health_check(self) -> bool:
        try:
            self._get_client()
            return True
        except Exception:
            return False

    @property
    def name(self) -> str:
        return "anthropic"


class OllamaProvider(BaseLLMProvider):
    """Ollama local model provider"""

    def __init__(self, base_url: str = "http://localhost:11434", model: str = "llama3"):
        self.base_url = base_url.rstrip("/")
        self.model = model

    def generate(self, messages: list[dict], **kwargs) -> str:
        import requests

        response = requests.post(
            f"{self.base_url}/api/chat",
            json={
                "model": self.model,
                "messages": messages,
                "stream": False,
                "options": {
                    "temperature": kwargs.get("temperature", 0.1),
                    "num_predict": kwargs.get("max_tokens", 1024),
                },
            },
            timeout=60,
        )
        response.raise_for_status()
        return response.json()["message"]["content"]

    def health_check(self) -> bool:
        try:
            import requests
            resp = requests.get(f"{self.base_url}/api/tags", timeout=5)
            return resp.status_code == 200
        except Exception:
            return False

    @property
    def name(self) -> str:
        return "ollama"
