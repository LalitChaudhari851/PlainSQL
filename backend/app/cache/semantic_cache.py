"""
Semantic Cache — Cache query results by embedding similarity.

Instead of requiring exact string matches, this cache finds semantically
similar queries that have already been processed. This means:
  "Show top 5 employees by salary" → cache hit for
  "List the 5 highest paid employees"

Uses MiniLM-L6-v2 embeddings with cosine similarity threshold.
Falls back gracefully when sentence-transformers is unavailable.
"""

import time
import os
import threading
import structlog
from typing import Optional

logger = structlog.get_logger()


class SemanticCache:
    """
    Embedding-based query cache with TTL and similarity threshold.

    Architecture:
    - Encodes queries using MiniLM-L6-v2 (384-dim embeddings)
    - Stores (embedding, result, timestamp) tuples
    - On lookup, computes cosine similarity against all cached embeddings
    - Returns cached result if similarity >= threshold

    Thread-safe for concurrent FastAPI workers.
    """

    def __init__(
        self,
        similarity_threshold: float = 0.95,
        ttl_seconds: int = 300,
        max_entries: int = 500,
    ):
        self._threshold = similarity_threshold
        self._ttl = ttl_seconds
        self._max_entries = max_entries
        self._lock = threading.Lock()

        # Cache entries: list of (embedding, result, timestamp, original_query)
        self._entries: list[tuple] = []

        # Lazy-load encoder
        self._encoder = None
        self._encoder_loaded = False

    def _get_encoder(self):
        """Lazy-load the sentence encoder on first use."""
        if os.environ.get("DISABLE_ML_INTENT", "false").lower() in ("true", "1", "yes"):
            logger.info("semantic_cache_encoder_disabled_by_env")
            return None

        if not self._encoder_loaded:
            self._encoder_loaded = True
            try:
                from sentence_transformers import SentenceTransformer
                self._encoder = SentenceTransformer("all-MiniLM-L6-v2")
                logger.info("semantic_cache_encoder_loaded", model="all-MiniLM-L6-v2")
            except ImportError:
                logger.info("semantic_cache_disabled",
                            reason="sentence-transformers not installed")
            except Exception as e:
                logger.warning("semantic_cache_encoder_failed", error=str(e))
        return self._encoder

    @property
    def available(self) -> bool:
        """Check if semantic caching is available."""
        if os.environ.get("DISABLE_ML_INTENT", "false").lower() in ("true", "1", "yes"):
            return False
        try:
            import sentence_transformers
            import numpy
            # Reference modules to satisfy unused import checks
            _ = sentence_transformers
            _ = numpy
            return True
        except ImportError:
            return False

    def get(self, query: str, tenant_id: str = "default") -> Optional[dict]:
        """
        Look up a semantically similar cached result.

        Returns the cached result dict if found, None otherwise.
        """
        encoder = self._get_encoder()
        if not encoder:
            return None

        try:
            import numpy as np

            query_emb = encoder.encode([query])[0]
            now = time.time()

            with self._lock:
                # Clean expired entries while searching
                valid_entries = []
                best_match = None
                best_score = 0.0

                for entry in self._entries:
                    emb, result, ts, original, tid = entry

                    # Skip expired
                    if now - ts > self._ttl:
                        continue

                    # Skip different tenant
                    if tid != tenant_id:
                        valid_entries.append(entry)
                        continue

                    valid_entries.append(entry)

                    # Cosine similarity
                    score = float(
                        np.dot(query_emb, emb)
                        / (np.linalg.norm(query_emb) * np.linalg.norm(emb) + 1e-8)
                    )

                    if score >= self._threshold and score > best_score:
                        best_score = score
                        best_match = result

                # Update entries (removes expired)
                self._entries = valid_entries

            if best_match:
                logger.info(
                    "semantic_cache_hit",
                    query=query[:60],
                    similarity=round(best_score, 3),
                )
                return best_match

        except Exception as e:
            logger.warning("semantic_cache_get_failed", error=str(e))

        return None

    def set(self, query: str, result: dict, tenant_id: str = "default"):
        """
        Cache a query result with its embedding.
        """
        encoder = self._get_encoder()
        if not encoder:
            return

        try:
            embedding = encoder.encode([query])[0]

            with self._lock:
                # Evict oldest if at capacity
                if len(self._entries) >= self._max_entries:
                    self._entries = self._entries[-(self._max_entries // 2):]

                self._entries.append(
                    (embedding, result, time.time(), query, tenant_id)
                )

            logger.debug("semantic_cache_set", query=query[:60])

        except Exception as e:
            logger.warning("semantic_cache_set_failed", error=str(e))

    def invalidate(self, tenant_id: str = "default"):
        """Clear all entries for a tenant."""
        with self._lock:
            self._entries = [
                e for e in self._entries if e[4] != tenant_id
            ]

    def invalidate_all(self):
        """Clear all cache entries."""
        with self._lock:
            self._entries.clear()

    def stats(self) -> dict:
        """Return cache statistics."""
        with self._lock:
            now = time.time()
            active = sum(1 for e in self._entries if now - e[2] <= self._ttl)
            return {
                "total_entries": len(self._entries),
                "active_entries": active,
                "max_entries": self._max_entries,
                "ttl_seconds": self._ttl,
                "similarity_threshold": self._threshold,
                "encoder_available": self._encoder is not None,
            }
