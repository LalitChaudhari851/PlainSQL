"""
Hybrid Retriever — Combines vector search (ChromaDB) with keyword search (BM25).
Uses Reciprocal Rank Fusion to merge results from both retrieval methods.
"""

import os

import chromadb
import structlog
from rank_bm25 import BM25Okapi
from typing import Optional

from app.rag.schema_enricher import SchemaEnricher

logger = structlog.get_logger()


class NoopCollection:
    """Minimal Chroma collection stand-in for local startup without vector search."""

    def count(self) -> int:
        return 0

    def get(self) -> dict:
        return {"ids": []}

    def delete(self, ids: list[str]):
        return None

    def add(self, documents: list[str], metadatas: list[dict], ids: list[str]):
        return None

    def query(self, query_texts: list[str], n_results: int) -> dict:
        return {"documents": [[]]}


class HybridRetriever:
    """
    Production RAG retriever combining:
    1. ChromaDB vector similarity (semantic search)
    2. BM25 keyword matching (exact table/column name matching)
    3. Reciprocal Rank Fusion to merge results
    """

    def __init__(self, db_pool, chroma_persist_dir: str = "./chroma_db"):
        self.db_pool = db_pool
        self.enricher = SchemaEnricher(db_pool)
        self.vector_enabled = os.getenv("DISABLE_VECTOR_RAG", "").lower() not in {"1", "true", "yes"}

        # Initialize ChromaDB unless local dev explicitly disables vector search.
        if self.vector_enabled:
            self.chroma_client = chromadb.PersistentClient(path=chroma_persist_dir)
            self.collection = self.chroma_client.get_or_create_collection(
                name="schema_knowledge_v2",
                metadata={"hnsw:space": "cosine"},
            )
        else:
            self.chroma_client = None
            self.collection = NoopCollection()
            logger.warning("vector_rag_disabled")

        # Document store for BM25
        self.documents: list[str] = []
        self.doc_ids: list[str] = []
        self.bm25: Optional[BM25Okapi] = None

        # Index on startup — file-locked to prevent SQLite race when
        # multiple Gunicorn workers start simultaneously.
        self._index_schema_safe(chroma_persist_dir)

    def _index_schema_safe(self, chroma_persist_dir: str):
        """Index schema with a file lock to prevent concurrent writes."""
        lock_path = os.path.join(chroma_persist_dir, ".index_lock")
        try:
            import filelock
            lock = filelock.FileLock(lock_path, timeout=30)
            with lock:
                self._index_schema()
        except ImportError:
            # filelock not installed — proceed without locking (single-worker is fine)
            logger.warning("filelock_not_installed", hint="pip install filelock for multi-worker safety")
            self._index_schema()
        except filelock.Timeout:
            logger.error("index_lock_timeout", lock_path=lock_path)
            self._index_schema()  # Proceed anyway

    def _index_schema(self):
        """Index all tables into both ChromaDB and BM25."""
        try:
            enriched_tables = self.enricher.enrich_all_tables()

            if not enriched_tables:
                logger.warning("no_tables_to_index")
                return

            documents = []
            metadatas = []
            ids = []

            for item in enriched_tables:
                documents.append(item["document"])
                metadatas.append(item["metadata"])
                ids.append(item["table_name"])

            if self.vector_enabled:
                # Clear existing ChromaDB data
                if self.collection.count() > 0:
                    existing = self.collection.get()
                    if existing["ids"]:
                        self.collection.delete(ids=existing["ids"])

                # Index into ChromaDB
                self.collection.add(
                    documents=documents,
                    metadatas=metadatas,
                    ids=ids,
                )

            # Build BM25 index
            self.documents = documents
            self.doc_ids = ids
            tokenized = [doc.lower().split() for doc in documents]
            self.bm25 = BM25Okapi(tokenized)

            logger.info(
                "schema_indexed",
                tables=len(enriched_tables),
                chroma_count=self.collection.count(),
            )

        except Exception as e:
            logger.error("schema_indexing_failed", error=str(e))

    def retrieve(self, query: str, top_k: int = 5) -> list[str]:
        """
        Retrieve relevant schema documents using hybrid search.
        Combines ChromaDB vector search with BM25 keyword search.
        """
        if not self.documents:
            logger.warning("empty_index_fallback")
            return [self.db_pool.get_full_schema()]

        try:
            # ── Vector search (ChromaDB) ─────────────────
            vector_docs = self._vector_search(query, top_k)

            # ── Keyword search (BM25) ────────────────────
            bm25_docs = self._keyword_search(query, top_k)

            # ── Reciprocal Rank Fusion ───────────────────
            merged = self._rrf_merge(vector_docs, bm25_docs, top_k)

            if not merged:
                # Fallback: return all documents
                return self.documents

            logger.info("retrieval_complete", vector_count=len(vector_docs), bm25_count=len(bm25_docs), merged_count=len(merged))

            return merged

        except Exception as e:
            logger.error("retrieval_failed", error=str(e))
            return [self.db_pool.get_full_schema()]

    def _vector_search(self, query: str, top_k: int) -> list[str]:
        """ChromaDB semantic similarity search."""
        if not self.vector_enabled:
            return []

        try:
            results = self.collection.query(
                query_texts=[query],
                n_results=min(top_k, self.collection.count()),
            )
            return results["documents"][0] if results["documents"] else []
        except Exception as e:
            logger.warning("vector_search_failed", error=str(e))
            return []

    def _keyword_search(self, query: str, top_k: int) -> list[str]:
        """BM25 keyword search for exact table/column name matching."""
        if not self.bm25:
            return []

        try:
            tokenized_query = query.lower().split()
            scores = self.bm25.get_scores(tokenized_query)

            # Get top-k indices sorted by score
            top_indices = sorted(
                range(len(scores)),
                key=lambda i: scores[i],
                reverse=True,
            )[:top_k]

            # Filter out zero-score results
            return [
                self.documents[i]
                for i in top_indices
                if scores[i] > 0
            ]
        except Exception as e:
            logger.warning("bm25_search_failed", error=str(e))
            return []

    @staticmethod
    def _rrf_merge(list_a: list[str], list_b: list[str], top_k: int, k: int = 60) -> list[str]:
        """
        Reciprocal Rank Fusion — merges two ranked lists.
        RRF score = Σ 1/(k + rank) for each list the document appears in.
        k=60 is the standard constant from the original RRF paper.
        """
        scores: dict[str, float] = {}

        for rank, doc in enumerate(list_a):
            scores[doc] = scores.get(doc, 0) + 1.0 / (k + rank + 1)

        for rank, doc in enumerate(list_b):
            scores[doc] = scores.get(doc, 0) + 1.0 / (k + rank + 1)

        sorted_docs = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        return [doc for doc, _ in sorted_docs[:top_k]]

    def refresh_index(self):
        """Re-index the schema (call after schema changes)."""
        logger.info("reindexing_schema")
        self._index_schema()
