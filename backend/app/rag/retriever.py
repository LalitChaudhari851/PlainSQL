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
    4. Optional cross-encoder reranking for precision
    """

    def __init__(self, db_pool, chroma_persist_dir: str = "./chroma_db"):
        self.db_pool = db_pool
        self.enricher = SchemaEnricher(db_pool)
        self.vector_enabled = os.getenv("DISABLE_VECTOR_RAG", "").lower() not in {"1", "true", "yes"}

        # Initialize ChromaDB unless local dev explicitly disables vector search.
        if self.vector_enabled:
            self.chroma_client = chromadb.PersistentClient(path=chroma_persist_dir)
            
            # Use Hugging Face Inference API embeddings if token is configured
            # to avoid loading heavy local PyTorch/ONNX models.
            embedding_fn = None
            hf_token = os.getenv("HUGGINGFACEHUB_API_TOKEN")
            if hf_token:
                try:
                    from chromadb.utils.embedding_functions import HuggingFaceEmbeddingFunction
                    embedding_fn = HuggingFaceEmbeddingFunction(
                        api_key=hf_token,
                        model_name="sentence-transformers/all-MiniLM-L6-v2"
                    )
                    logger.info("using_huggingface_embedding_function")
                except Exception as e:
                    logger.warning("hf_embedding_init_failed", error=str(e))

            self.collection = self.chroma_client.get_or_create_collection(
                name="schema_knowledge_v2",
                metadata={"hnsw:space": "cosine"},
                embedding_function=embedding_fn,
            )
        else:
            self.chroma_client = None
            self.collection = NoopCollection()
            logger.warning("vector_rag_disabled")

        # Document store for BM25
        self.documents: list[str] = []
        self.doc_ids: list[str] = []
        self.bm25: Optional[BM25Okapi] = None

        # Optional cross-encoder reranker — lazy-loaded on first use
        self._reranker = None
        self._reranker_loaded = False

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
            schema_file = os.path.join(self.chroma_client.persist_directory if self.chroma_client and hasattr(self.chroma_client, "persist_directory") else "./chroma_db", "enriched_schema.json")
            
            # Try to load from file cache first to avoid startup DB queries
            loaded_from_cache = False
            enriched_tables = []
            
            if os.path.exists(schema_file):
                try:
                    import json
                    with open(schema_file, "r", encoding="utf-8") as f:
                        enriched_tables = json.load(f)
                    loaded_from_cache = True
                    logger.info("loaded_schema_from_file_cache", path=schema_file)
                except Exception as e:
                    logger.warning("failed_to_load_schema_cache", error=str(e))
            
            if not loaded_from_cache:
                enriched_tables = self.enricher.enrich_all_tables()
                if enriched_tables:
                    try:
                        import json
                        os.makedirs(os.path.dirname(schema_file), exist_ok=True)
                        with open(schema_file, "w", encoding="utf-8") as f:
                            json.dump(enriched_tables, f, ensure_ascii=False, indent=2)
                        logger.info("saved_schema_to_file_cache", path=schema_file)
                    except Exception as e:
                        logger.warning("failed_to_save_schema_cache", error=str(e))

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
                current_count = self.collection.count()
                if current_count == len(enriched_tables):
                    logger.info("schema_already_indexed_skipping_write", count=current_count)
                else:
                    # Clear existing ChromaDB data
                    if current_count > 0:
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
        Combines ChromaDB vector search with BM25 keyword search,
        then optionally reranks with a cross-encoder for precision.
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
            # Fetch more candidates than needed for reranking
            merge_k = top_k * 2 if self._get_reranker() else top_k
            merged = self._rrf_merge(vector_docs, bm25_docs, merge_k)

            if not merged:
                # Fallback: return all documents
                return self.documents

            # ── Cross-encoder reranking (optional) ───────
            reranked = False
            reranker = self._get_reranker()
            if reranker and len(merged) > top_k:
                merged = self._rerank(query, merged, top_k)
                reranked = True

            result = merged[:top_k]

            logger.info(
                "retrieval_complete",
                vector_count=len(vector_docs),
                bm25_count=len(bm25_docs),
                merged_count=len(result),
                reranked=reranked,
            )

            return result

        except Exception as e:
            logger.error("retrieval_failed", error=str(e))
            return [self.db_pool.get_full_schema()]

    def retrieve_expanded(self, query: str, entities: list[str] = None, top_k: int = 5) -> list[str]:
        """
        Retrieve with query expansion for better recall on multi-table queries.

        When a user says 'revenue by region', the system needs both the sales
        and customers tables. Query expansion generates multiple search queries
        to ensure all needed schemas are retrieved.

        Falls back to standard retrieve() if entities are empty.
        """
        if not entities or len(entities) <= 1:
            return self.retrieve(query, top_k)

        try:
            expanded_queries = self._expand_query(query, entities)
            all_docs = []
            seen_hashes = set()

            for q in expanded_queries:
                docs = self.retrieve(q, top_k=top_k)
                for doc in docs:
                    doc_hash = hash(doc[:200])  # Hash first 200 chars for dedup
                    if doc_hash not in seen_hashes:
                        seen_hashes.add(doc_hash)
                        all_docs.append(doc)

            # Rerank the combined set to select the best top_k
            reranker = self._get_reranker()
            if reranker and len(all_docs) > top_k:
                all_docs = self._rerank(query, all_docs, top_k)

            result = all_docs[:top_k]

            logger.info(
                "expanded_retrieval_complete",
                num_queries=len(expanded_queries),
                total_candidates=len(all_docs),
                returned=len(result),
            )

            return result

        except Exception as e:
            logger.warning("expanded_retrieval_failed", error=str(e))
            return self.retrieve(query, top_k)

    @staticmethod
    def _expand_query(query: str, entities: list[str]) -> list[str]:
        """
        Generate multiple search queries for better recall.

        Strategy:
        1. Original query (captures user intent)
        2. Per-entity queries (ensures each table's schema is searched)
        3. Relationship query (helps find JOIN paths between entities)
        """
        queries = [query]

        # Per-entity focused queries
        for entity in entities:
            queries.append(f"{entity} table schema columns relationships")

        # Cross-entity relationship query
        if len(entities) > 1:
            queries.append(f"relationship between {' and '.join(entities)} foreign key join")

        return queries

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

    def _get_reranker(self):
        """Lazy-load the cross-encoder reranker."""
        if os.environ.get("DISABLE_ML_INTENT", "false").lower() in ("true", "1", "yes") or not self.vector_enabled:
            return None

        if self._reranker_loaded:
            return self._reranker

        self._reranker_loaded = True
        try:
            from sentence_transformers import CrossEncoder
            self._reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
            logger.info("cross_encoder_reranker_loaded")
        except ImportError:
            logger.info("reranker_unavailable", hint="pip install sentence-transformers for reranking")
        except Exception as e:
            logger.warning("reranker_load_failed", error=str(e))

        return self._reranker

    def _rerank(self, query: str, docs: list[str], top_k: int) -> list[str]:
        """Rerank documents using cross-encoder for precise relevance scoring."""
        try:
            pairs = [(query, doc) for doc in docs]
            scores = self._reranker.predict(pairs)
            ranked = sorted(zip(docs, scores), key=lambda x: x[1], reverse=True)
            return [doc for doc, _ in ranked[:top_k]]
        except Exception as e:
            logger.warning("reranking_failed", error=str(e))
            return docs[:top_k]

    def refresh_index(self):
        """Re-index the schema (call after schema changes)."""
        logger.info("reindexing_schema")
        if hasattr(self.db_pool, 'clear_schema_cache'):
            self.db_pool.clear_schema_cache()
            
        schema_file = os.path.join(self.chroma_client.persist_directory if self.chroma_client and hasattr(self.chroma_client, "persist_directory") else "./chroma_db", "enriched_schema.json")
        if os.path.exists(schema_file):
            try:
                os.remove(schema_file)
                logger.info("deleted_schema_file_cache")
            except Exception as e:
                logger.warning("failed_to_delete_schema_file_cache", error=str(e))
                
        self._index_schema()
