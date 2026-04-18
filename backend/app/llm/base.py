"""
Abstract LLM Provider Interface — All providers must implement this.
"""

from abc import ABC, abstractmethod


class BaseLLMProvider(ABC):
    """Base class for all LLM providers."""

    @abstractmethod
    def generate(self, messages: list[dict], **kwargs) -> str:
        """Generate a response from the model."""
        ...

    @abstractmethod
    def health_check(self) -> bool:
        """Check if the provider is available."""
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name identifier."""
        ...
