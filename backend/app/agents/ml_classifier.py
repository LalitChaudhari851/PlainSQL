"""
ML Intent Classifier — Sentence-transformer embeddings + LogisticRegression.

Provides a trained ML model for intent classification, replacing pure heuristic
keyword matching with learned representations. Falls back gracefully when the
model file is not available.

Architecture:
    sentence-transformers/all-MiniLM-L6-v2 (384-dim) → LogisticRegression (4 classes)

Classes:
    chat        — Greetings, thanks, capability questions, off-topic
    sql         — Data queries, aggregations, comparisons, joins
    ambiguous   — Vague data-shaped queries that need clarification
    meta_query  — Schema exploration (show tables, describe columns)
"""

import os
import structlog
from typing import Optional
from dataclasses import dataclass

logger = structlog.get_logger()

# Path to the pre-trained model artifact
_MODEL_DIR = os.path.dirname(os.path.abspath(__file__))
_MODEL_PATH = os.path.join(_MODEL_DIR, "models", "intent_model.joblib")

# Map ML labels to route_intent values for pipeline compatibility
_ROUTE_INTENT_MAP = {
    "chat": "chat",
    "sql": "data_query",
    "ambiguous": "chat",
    "meta_query": "meta_query",
}


@dataclass
class MLClassification:
    """Result from the ML classifier."""
    intent: str          # chat | sql | ambiguous | meta_query
    route_intent: str    # Pipeline-compatible intent for routing
    confidence: float    # Model confidence (0.0 - 1.0)
    method: str = "ml"   # Always "ml" for this classifier


class MLIntentClassifier:
    """
    ML-based intent classifier using sentence-transformer embeddings
    and logistic regression.

    Loads a pre-trained model from disk. If the model is unavailable,
    classify() returns None so the caller can fall back to heuristics.
    """

    def __init__(self, model_path: str = None):
        self.model = None
        self.encoder = None
        self._loaded = False
        self._model_path = model_path or _MODEL_PATH

        self._try_load()

    def _try_load(self):
        """Attempt to load the pre-trained model and encoder."""
        if not os.path.exists(self._model_path):
            logger.info("ml_classifier_model_not_found", path=self._model_path)
            return

        try:
            import joblib
            from sentence_transformers import SentenceTransformer

            self.model = joblib.load(self._model_path)
            self.encoder = SentenceTransformer("all-MiniLM-L6-v2")
            self._loaded = True
            logger.info("ml_classifier_loaded", path=self._model_path)
        except ImportError as e:
            logger.warning("ml_classifier_deps_missing", error=str(e),
                           hint="pip install scikit-learn sentence-transformers joblib")
        except Exception as e:
            logger.warning("ml_classifier_load_failed", error=str(e))

    @property
    def available(self) -> bool:
        """Whether the ML model is loaded and ready for inference."""
        return self._loaded and self.model is not None and self.encoder is not None

    def classify(self, query: str) -> Optional[MLClassification]:
        """
        Classify a query using the ML model.

        Returns:
            MLClassification if model is available and confident,
            None if model is unavailable (caller should fall back to heuristic).
        """
        if not self.available:
            return None

        try:
            # Encode the query to a 384-dim embedding
            embedding = self.encoder.encode([query])

            # Predict class and confidence
            predicted_label = self.model.predict(embedding)[0]
            probabilities = self.model.predict_proba(embedding)[0]
            confidence = float(max(probabilities))

            route_intent = _ROUTE_INTENT_MAP.get(predicted_label, "data_query")

            logger.debug(
                "ml_classification",
                query=query[:80],
                label=predicted_label,
                confidence=round(confidence, 3),
                route_intent=route_intent,
            )

            return MLClassification(
                intent=predicted_label,
                route_intent=route_intent,
                confidence=confidence,
            )

        except Exception as e:
            logger.warning("ml_classification_failed", error=str(e))
            return None


# ── Module-level singleton ───────────────────────────────────
# Lazy-loaded on first use to avoid startup cost if not needed.
_classifier_instance: Optional[MLIntentClassifier] = None


def get_ml_classifier() -> MLIntentClassifier:
    """Get or create the singleton ML classifier instance."""
    global _classifier_instance
    if _classifier_instance is None:
        _classifier_instance = MLIntentClassifier()
    return _classifier_instance
