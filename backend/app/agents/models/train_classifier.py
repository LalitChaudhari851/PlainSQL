"""
Train the ML intent classifier.

Usage:
    cd backend
    python -m app.agents.models.train_classifier

This script:
1. Loads training data from training_data.json
2. Encodes all examples using sentence-transformers/all-MiniLM-L6-v2
3. Trains a LogisticRegression classifier with cross-validation
4. Saves the model to intent_model.joblib
5. Prints a classification report and accuracy metrics

The output model is used by MLIntentClassifier in ml_classifier.py.
"""

import json
import os
import sys
import numpy as np


def main():
    # ── Load training data ───────────────────────────────
    data_dir = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(data_dir, "training_data.json")
    model_path = os.path.join(data_dir, "intent_model.joblib")

    print(f"[LOAD] Loading training data from {data_path}")
    with open(data_path, "r") as f:
        dataset = json.load(f)

    texts = [item["text"] for item in dataset]
    labels = [item["label"] for item in dataset]

    print(f"   Total examples: {len(texts)}")
    label_counts = {}
    for label in labels:
        label_counts[label] = label_counts.get(label, 0) + 1
    for label, count in sorted(label_counts.items()):
        print(f"   - {label}: {count}")

    # ── Encode with sentence-transformers ────────────────
    print(f"\n[ENCODE] Encoding with sentence-transformers/all-MiniLM-L6-v2...")
    from sentence_transformers import SentenceTransformer

    encoder = SentenceTransformer("all-MiniLM-L6-v2")
    embeddings = encoder.encode(texts, show_progress_bar=True)
    print(f"   Embedding shape: {embeddings.shape}")

    # ── Train/test split ────────────────────────────────
    from sklearn.model_selection import train_test_split, cross_val_score
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import classification_report, confusion_matrix

    X_train, X_test, y_train, y_test = train_test_split(
        embeddings, labels, test_size=0.2, random_state=42, stratify=labels
    )

    print(f"\n[SPLIT] Train: {len(X_train)} | Test: {len(X_test)}")

    # ── Train LogisticRegression ────────────────────────
    print(f"\n[TRAIN] Training LogisticRegression classifier...")
    model = LogisticRegression(
        max_iter=1000,
        C=1.0,
        solver="lbfgs",
        multi_class="multinomial",
        class_weight="balanced",  # Handle class imbalance
        random_state=42,
    )
    model.fit(X_train, y_train)

    # ── Evaluate ────────────────────────────────────────
    y_pred = model.predict(X_test)
    accuracy = (np.array(y_pred) == np.array(y_test)).mean()

    print(f"\n{'='*60}")
    print(f"  TEST SET RESULTS")
    print(f"{'='*60}")
    print(f"   Accuracy: {accuracy:.1%}")
    print(f"\n{classification_report(y_test, y_pred)}")

    # ── Cross-validation ────────────────────────────────
    print(f"[CV] 5-Fold Cross-Validation...")
    cv_scores = cross_val_score(model, embeddings, labels, cv=5, scoring="accuracy")
    print(f"   CV Accuracy: {cv_scores.mean():.1%} +/- {cv_scores.std():.1%}")
    print(f"   Fold scores: {[f'{s:.1%}' for s in cv_scores]}")

    # ── Confusion Matrix ────────────────────────────────
    print(f"\n[MATRIX] Confusion Matrix:")
    cm = confusion_matrix(y_test, y_pred, labels=sorted(set(labels)))
    cm_labels = sorted(set(labels))
    header = "          " + "  ".join(f"{l:>10}" for l in cm_labels)
    print(header)
    for i, row in enumerate(cm):
        row_str = "  ".join(f"{v:>10}" for v in row)
        print(f"  {cm_labels[i]:>8} {row_str}")

    # ── Save model ──────────────────────────────────────
    print(f"\n[SAVE] Saving model to {model_path}")
    import joblib
    joblib.dump(model, model_path)
    model_size = os.path.getsize(model_path)
    print(f"   Model size: {model_size / 1024:.1f} KB")

    # ── Quick inference test ────────────────────────────
    print(f"\n[TEST] Quick inference test:")
    test_queries = [
        "hello",
        "show top 5 employees by salary",
        "what tables are there",
        "show me some data",
        "total revenue by region",
        "thanks",
    ]
    test_embeddings = encoder.encode(test_queries)
    test_preds = model.predict(test_embeddings)
    test_probs = model.predict_proba(test_embeddings)

    for query, pred, probs in zip(test_queries, test_preds, test_probs):
        conf = max(probs)
        print(f"   '{query}' -> {pred} ({conf:.0%})")

    print(f"\n[DONE] Training complete!")
    return accuracy


if __name__ == "__main__":
    main()
