"""
Request Deduplication — Prevents duplicate in-flight LLM executions.

If two users send the same query within a short window, the second request
attaches to the result of the first instead of spawning a new LLM call.

Uses an in-memory dict with threading locks. For multi-worker deployments,
upgrade to Redis-based dedup using SETNX.
"""

import hashlib
import threading
import time
import structlog
from typing import Optional

logger = structlog.get_logger()

# Maximum time (seconds) a dedup entry stays alive before cleanup
_DEDUP_TTL = 120


class RequestDeduplicator:
    """
    Tracks in-flight queries by content hash.
    Returns a cached result if the same query is already being processed.
    Thread-safe for single-worker deployments.
    """

    def __init__(self, ttl: int = _DEDUP_TTL):
        self.ttl = ttl
        self._inflight: dict[str, dict] = {}  # hash -> {status, result, timestamp, event}
        self._lock = threading.Lock()

    @staticmethod
    def _hash_query(query: str, user_id: str = "anonymous") -> str:
        """Create a content-addressable hash for the query."""
        raw = f"{user_id}:{query.strip().lower()}"
        return hashlib.sha256(raw.encode()).hexdigest()[:20]

    def try_acquire(self, query: str, user_id: str = "anonymous") -> tuple[bool, str]:
        """
        Try to claim a dedup slot for this query.

        Returns:
            (is_new, query_hash)
            - is_new=True: This is the first request. Caller should execute the pipeline.
            - is_new=False: Another request is already processing this query.
        """
        query_hash = self._hash_query(query, user_id)

        with self._lock:
            self._cleanup_stale()

            if query_hash in self._inflight:
                entry = self._inflight[query_hash]
                if entry["status"] == "running":
                    logger.info("dedup_hit", query_hash=query_hash)
                    return False, query_hash

            # Claim the slot
            self._inflight[query_hash] = {
                "status": "running",
                "result": None,
                "timestamp": time.time(),
                "event": threading.Event(),
            }
            return True, query_hash

    def complete(self, query_hash: str, result: dict):
        """Mark a query as completed and store the result for waiting clients."""
        with self._lock:
            if query_hash in self._inflight:
                self._inflight[query_hash]["status"] = "done"
                self._inflight[query_hash]["result"] = result
                self._inflight[query_hash]["event"].set()

    def wait_for_result(self, query_hash: str, timeout: float = 65.0) -> Optional[dict]:
        """Wait for an in-flight query to complete and return its result."""
        entry = None
        with self._lock:
            entry = self._inflight.get(query_hash)

        if not entry:
            return None

        # Wait for the event (blocking but with timeout)
        entry["event"].wait(timeout=timeout)
        return entry.get("result")

    def release(self, query_hash: str):
        """Release a dedup slot (called on error or after consumers are done)."""
        with self._lock:
            self._inflight.pop(query_hash, None)

    def _cleanup_stale(self):
        """Remove entries older than TTL to prevent memory leaks."""
        now = time.time()
        stale = [k for k, v in self._inflight.items() if now - v["timestamp"] > self.ttl]
        for k in stale:
            # Signal any waiters before removing
            self._inflight[k]["event"].set()
            del self._inflight[k]
        if stale:
            logger.debug("dedup_cleanup", removed=len(stale))

    @property
    def inflight_count(self) -> int:
        """Number of currently in-flight queries."""
        with self._lock:
            return sum(1 for v in self._inflight.values() if v["status"] == "running")
