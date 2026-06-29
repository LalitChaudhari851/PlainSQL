"""
Dynamic Few-Shot Selector — Selects the most relevant SQL examples for each query.

Instead of hardcoded few-shot examples, this module uses embedding similarity
to find the most relevant examples from the evaluation dataset. This improves
SQL generation accuracy by giving the LLM contextually similar reference queries.

Architecture:
    Query → encode with MiniLM → cosine similarity against example embeddings → top-k
"""

import json
import os
import structlog
import numpy as np
from typing import Optional

logger = structlog.get_logger()

# Default path: train split
_EVAL_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "evaluation",
)
_DEFAULT_EXAMPLES_PATH = os.path.join(_EVAL_DIR, "datasets", "train.json")


class DynamicFewShotSelector:
    """
    Select the most relevant few-shot examples for each user query
    using embedding similarity.

    Lazy-loads the encoder and example embeddings on first use to avoid
    slowing down application startup.
    """

    def __init__(self, examples_path: str = None):
        self._examples_path = examples_path or _DEFAULT_EXAMPLES_PATH
        self._encoder = None
        self._examples: list[dict] = []
        self._embeddings: Optional[np.ndarray] = None
        self._loaded = False

    def _lazy_load(self):
        """Load encoder and examples on first use."""
        if self._loaded:
            return

        self._loaded = True  # Prevent retry loops on failure

        # Load examples from dataset
        if not os.path.exists(self._examples_path):
            logger.info("few_shot_dataset_not_found", path=self._examples_path)
            return

        try:
            with open(self._examples_path, "r") as f:
                self._examples = json.load(f)
            logger.info("few_shot_examples_loaded", count=len(self._examples))
        except Exception as e:
            logger.warning("few_shot_load_failed", error=str(e))
            return

        # Load encoder and pre-compute embeddings
        if os.environ.get("DISABLE_ML_INTENT", "false").lower() in ("true", "1", "yes"):
            logger.info("few_shot_encoder_disabled_by_env")
            return

        try:
            from sentence_transformers import SentenceTransformer

            self._encoder = SentenceTransformer("all-MiniLM-L6-v2")
            questions = [ex["question"] for ex in self._examples]
            self._embeddings = self._encoder.encode(questions, show_progress_bar=False)
            logger.info("few_shot_embeddings_computed", count=len(questions))
        except ImportError:
            logger.info("few_shot_encoder_unavailable", hint="pip install sentence-transformers")
        except Exception as e:
            logger.warning("few_shot_encoding_failed", error=str(e))

    @property
    def available(self) -> bool:
        """Whether the selector is ready for use."""
        self._lazy_load()
        return (
            self._encoder is not None
            and self._embeddings is not None
            and len(self._examples) > 0
        )

    def select(self, query: str, k: int = 3) -> list[dict]:
        """
        Select k most similar examples to the query.

        Args:
            query: The user's natural language question.
            k: Number of examples to return.

        Returns:
            List of example dicts with 'question' and 'expected_sql' keys.
            Returns empty list if the selector is unavailable.
        """
        if not self.available:
            return []

        try:
            query_emb = self._encoder.encode([query], show_progress_bar=False)[0]

            # Cosine similarity against all examples
            norms = np.linalg.norm(self._embeddings, axis=1) * np.linalg.norm(query_emb)
            # Guard against zero norms
            norms = np.where(norms == 0, 1e-10, norms)
            similarities = np.dot(self._embeddings, query_emb) / norms

            # Get indices sorted by similarity (highest first)
            top_indices = np.argsort(similarities)[::-1]

            # Select top-k, excluding exact query matches to prevent data leakage during eval
            selected = []
            query_lower = query.lower().strip()
            for idx in top_indices:
                ex = self._examples[idx]
                if ex["question"].lower().strip() != query_lower:
                    selected.append(ex)
                    if len(selected) >= k:
                        break

            return selected

        except Exception as e:
            logger.warning("few_shot_selection_failed", error=str(e))
            return []

    def format_for_prompt(self, examples: list[dict]) -> str:
        """
        Format selected examples as text to inject into the prompt.

        Args:
            examples: List of example dicts from select().

        Returns:
            Formatted string for prompt injection, or empty string if no examples.
        """
        if not examples:
            return ""

        parts = ["\n## SIMILAR EXAMPLES FROM EVALUATION SET"]
        for i, ex in enumerate(examples, 1):
            parts.append(f"\nSimilar Query {i}:")
            parts.append(f'Q: "{ex["question"]}"')
            parts.append(f'SQL: {ex["expected_sql"]}')

        return "\n".join(parts)


# ── Module-level singleton ───────────────────────────────
_selector_instance: Optional[DynamicFewShotSelector] = None


def get_few_shot_selector() -> DynamicFewShotSelector:
    """Get or create the singleton few-shot selector instance."""
    global _selector_instance
    if _selector_instance is None:
        _selector_instance = DynamicFewShotSelector()
    return _selector_instance
