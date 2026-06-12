"""
Tests for the ML Intent Classifier.
Validates model loading, inference, confidence thresholds, and graceful fallback.
"""

import sys
import os
import json
import pytest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestMLClassifierFallback:
    """Test that the system gracefully falls back when ML model is unavailable."""

    def test_classifier_returns_none_when_model_missing(self):
        """When model file doesn't exist, classify() should return None."""
        from app.agents.ml_classifier import MLIntentClassifier
        clf = MLIntentClassifier(model_path="/nonexistent/path/model.joblib")
        assert clf.available is False
        assert clf.classify("hello") is None

    def test_heuristic_still_works_without_ml(self):
        """Intent classifier should work even when ML model is not available."""
        from app.agents.intent_classifier import classify_intent
        # These should all work via heuristic regardless of ML availability
        result = classify_intent("hello")
        assert result.intent == "chat"

        result = classify_intent("show top 5 employees by salary")
        assert result.intent == "sql"

    def test_greeting_still_chat_without_ml(self):
        from app.agents.intent_classifier import classify_intent
        for greeting in ["hi", "hey", "thanks", "bye", "what can you do"]:
            result = classify_intent(greeting)
            assert result.intent == "chat", f"'{greeting}' should be chat, got {result.intent}"

    def test_sql_queries_still_work_without_ml(self):
        from app.agents.intent_classifier import classify_intent
        sql_queries = [
            "total sales revenue by region",
            "how many employees are in each department",
            "show products with low stock",
        ]
        for q in sql_queries:
            result = classify_intent(q)
            assert result.intent == "sql", f"'{q}' should be sql, got {result.intent}"


class TestMLClassifierIntegration:
    """
    Integration tests that run when the model file exists.
    These are skipped if the model hasn't been trained yet.
    """

    @staticmethod
    def _model_exists():
        model_path = os.path.join(
            os.path.dirname(__file__), "..", "app", "agents", "models", "intent_model.joblib"
        )
        return os.path.exists(model_path)

    @pytest.mark.skipif(
        not os.path.exists(
            os.path.join(os.path.dirname(__file__), "..", "app", "agents", "models", "intent_model.joblib")
        ),
        reason="ML model not trained yet — run: python -m app.agents.models.train_classifier",
    )
    def test_ml_model_loads(self):
        """Verify the trained model loads successfully."""
        from app.agents.ml_classifier import MLIntentClassifier
        clf = MLIntentClassifier()
        assert clf.available is True

    @pytest.mark.skipif(
        not os.path.exists(
            os.path.join(os.path.dirname(__file__), "..", "app", "agents", "models", "intent_model.joblib")
        ),
        reason="ML model not trained yet",
    )
    def test_ml_classifies_greeting_as_chat(self):
        from app.agents.ml_classifier import MLIntentClassifier
        clf = MLIntentClassifier()
        result = clf.classify("hello how are you")
        assert result is not None
        assert result.intent == "chat"
        assert result.confidence > 0.5

    @pytest.mark.skipif(
        not os.path.exists(
            os.path.join(os.path.dirname(__file__), "..", "app", "agents", "models", "intent_model.joblib")
        ),
        reason="ML model not trained yet",
    )
    def test_ml_classifies_sql_query(self):
        from app.agents.ml_classifier import MLIntentClassifier
        clf = MLIntentClassifier()
        result = clf.classify("show top 5 employees by salary")
        assert result is not None
        assert result.intent == "sql"
        assert result.confidence > 0.5

    @pytest.mark.skipif(
        not os.path.exists(
            os.path.join(os.path.dirname(__file__), "..", "app", "agents", "models", "intent_model.joblib")
        ),
        reason="ML model not trained yet",
    )
    def test_ml_classifies_meta_query(self):
        from app.agents.ml_classifier import MLIntentClassifier
        clf = MLIntentClassifier()
        result = clf.classify("what tables are in the database")
        assert result is not None
        assert result.intent == "meta_query"

    @pytest.mark.skipif(
        not os.path.exists(
            os.path.join(os.path.dirname(__file__), "..", "app", "agents", "models", "intent_model.joblib")
        ),
        reason="ML model not trained yet",
    )
    def test_ml_classification_has_confidence(self):
        from app.agents.ml_classifier import MLIntentClassifier
        clf = MLIntentClassifier()
        result = clf.classify("total revenue by region")
        assert result is not None
        assert 0.0 <= result.confidence <= 1.0
        assert result.method == "ml"


class TestMLClassificationBridge:
    """Test the _try_ml_classification bridge in intent_classifier.py."""

    def test_bridge_returns_none_when_ml_unavailable(self):
        """When ML model doesn't load, bridge returns None and heuristic runs."""
        from app.agents.intent_classifier import _try_ml_classification
        # Force a fresh import with no model
        with patch("app.agents.ml_classifier.get_ml_classifier") as mock_get:
            mock_clf = MagicMock()
            mock_clf.available = False
            mock_get.return_value = mock_clf
            result = _try_ml_classification("hello")
            assert result is None

    def test_bridge_returns_none_on_low_confidence(self):
        """When ML confidence is below threshold, bridge returns None."""
        from app.agents.intent_classifier import _try_ml_classification
        from app.agents.ml_classifier import MLClassification

        with patch("app.agents.ml_classifier.get_ml_classifier") as mock_get:
            mock_clf = MagicMock()
            mock_clf.available = True
            mock_clf.classify.return_value = MLClassification(
                intent="chat", route_intent="chat", confidence=0.45
            )
            mock_get.return_value = mock_clf
            result = _try_ml_classification("maybe this is chat")
            assert result is None  # Below 0.70 threshold

    def test_bridge_returns_classification_on_high_confidence(self):
        """When ML confidence is above threshold, bridge returns classification."""
        from app.agents.intent_classifier import _try_ml_classification
        from app.agents.ml_classifier import MLClassification

        with patch("app.agents.ml_classifier.get_ml_classifier") as mock_get:
            mock_clf = MagicMock()
            mock_clf.available = True
            mock_clf.classify.return_value = MLClassification(
                intent="sql", route_intent="data_query", confidence=0.92
            )
            mock_get.return_value = mock_clf
            result = _try_ml_classification("show top 5 employees by salary")
            assert result is not None
            assert result.intent == "sql"
            assert "ml_model" in result.reason


class TestTrainingData:
    """Validate the training dataset structure."""

    def test_training_data_loads(self):
        data_path = os.path.join(
            os.path.dirname(__file__), "..", "app", "agents", "models", "training_data.json"
        )
        with open(data_path, "r") as f:
            data = json.load(f)
        assert len(data) > 100, f"Expected 100+ training examples, got {len(data)}"

    def test_all_labels_valid(self):
        data_path = os.path.join(
            os.path.dirname(__file__), "..", "app", "agents", "models", "training_data.json"
        )
        with open(data_path, "r") as f:
            data = json.load(f)
        valid_labels = {"chat", "sql", "ambiguous", "meta_query"}
        for item in data:
            assert "text" in item, f"Missing 'text' field: {item}"
            assert "label" in item, f"Missing 'label' field: {item}"
            assert item["label"] in valid_labels, f"Invalid label '{item['label']}' in: {item}"

    def test_all_classes_represented(self):
        data_path = os.path.join(
            os.path.dirname(__file__), "..", "app", "agents", "models", "training_data.json"
        )
        with open(data_path, "r") as f:
            data = json.load(f)
        labels = {item["label"] for item in data}
        assert labels == {"chat", "sql", "ambiguous", "meta_query"}

    def test_minimum_per_class(self):
        """Each class should have at least 15 examples for meaningful training."""
        data_path = os.path.join(
            os.path.dirname(__file__), "..", "app", "agents", "models", "training_data.json"
        )
        with open(data_path, "r") as f:
            data = json.load(f)
        from collections import Counter
        counts = Counter(item["label"] for item in data)
        for label, count in counts.items():
            assert count >= 15, f"Class '{label}' has only {count} examples, need 15+"
