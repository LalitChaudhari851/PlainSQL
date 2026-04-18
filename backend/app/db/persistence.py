"""
Conversation Persistence — MySQL-backed chat history management.

Provides CRUD operations for conversations and messages, enabling
cross-device chat persistence and server-side state management.
"""

import uuid
from datetime import datetime
from typing import Optional
import structlog

logger = structlog.get_logger()

# ── Schema Migration ─────────────────────────────────────────

CONVERSATIONS_DDL = """
CREATE TABLE IF NOT EXISTS conversations (
    id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(64) NOT NULL DEFAULT 'anonymous',
    title VARCHAR(255) NOT NULL DEFAULT 'New analysis',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_user (user_id),
    INDEX idx_updated (updated_at DESC)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""

MESSAGES_DDL = """
CREATE TABLE IF NOT EXISTS messages (
    id VARCHAR(36) PRIMARY KEY,
    conversation_id VARCHAR(36) NOT NULL,
    role ENUM('user', 'assistant') NOT NULL,
    content TEXT,
    generated_sql TEXT,
    explanation TEXT,
    friendly_message TEXT,
    intent VARCHAR(32),
    execution_time_ms FLOAT DEFAULT 0,
    row_count INT DEFAULT 0,
    result_data JSON,
    feedback ENUM('up', 'down') DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_conversation (conversation_id),
    INDEX idx_created (created_at),
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
"""


def ensure_tables(db_pool) -> bool:
    """Auto-create conversations and messages tables on startup."""
    try:
        db_pool._execute_write_internal(CONVERSATIONS_DDL)
        db_pool._execute_write_internal(MESSAGES_DDL)
        logger.info("persistence_tables_ready")
        return True
    except Exception as e:
        logger.warning("persistence_migration_failed", error=str(e))
        return False


# ── Conversation Manager ─────────────────────────────────────

class ConversationManager:
    """
    Server-side conversation and message persistence.
    Replaces localStorage-based chat history with MySQL storage.
    """

    def __init__(self, db_pool):
        self.db_pool = db_pool

    # ── Conversations ────────────────────────────────────

    def list_conversations(self, user_id: str = "anonymous", limit: int = 50) -> list[dict]:
        """List conversations for a user, most recent first."""
        rows = self.db_pool.execute_query(
            """SELECT c.id, c.title, c.created_at, c.updated_at,
                      COUNT(m.id) as message_count
               FROM conversations c
               LEFT JOIN messages m ON m.conversation_id = c.id
               WHERE c.user_id = :user_id
               GROUP BY c.id
               ORDER BY c.updated_at DESC
               LIMIT :lim""",
            {"user_id": user_id, "lim": limit},
        )
        return [
            {
                "id": r["id"],
                "title": r["title"],
                "message_count": r["message_count"],
                "created_at": str(r["created_at"]),
                "updated_at": str(r["updated_at"]),
            }
            for r in rows
        ]

    def create_conversation(self, title: str = "New analysis", user_id: str = "anonymous") -> dict:
        """Create a new conversation and return its metadata."""
        conv_id = str(uuid.uuid4())[:36]
        self.db_pool._execute_write_internal(
            "INSERT INTO conversations (id, user_id, title) VALUES (:p0, :p1, :p2)",
            (conv_id, user_id, title[:255]),
        )
        logger.info("conversation_created", conversation_id=conv_id)
        return {"id": conv_id, "title": title, "message_count": 0}

    def update_title(self, conversation_id: str, title: str):
        """Update a conversation's title."""
        self.db_pool._execute_write_internal(
            "UPDATE conversations SET title = :p0 WHERE id = :p1",
            (title[:255], conversation_id),
        )

    def delete_conversation(self, conversation_id: str, user_id: str = "anonymous"):
        """Delete a conversation and all its messages (cascade)."""
        self.db_pool._execute_write_internal(
            "DELETE FROM conversations WHERE id = :p0 AND user_id = :p1",
            (conversation_id, user_id),
        )
        logger.info("conversation_deleted", conversation_id=conversation_id)

    # ── Messages ─────────────────────────────────────────

    def get_messages(self, conversation_id: str, limit: int = 200) -> list[dict]:
        """Get all messages in a conversation, oldest first."""
        rows = self.db_pool.execute_query(
            """SELECT id, role, content, generated_sql, explanation,
                      friendly_message, intent, execution_time_ms,
                      row_count, result_data, feedback, created_at
               FROM messages
               WHERE conversation_id = :conv_id
               ORDER BY created_at ASC
               LIMIT :lim""",
            {"conv_id": conversation_id, "lim": limit},
        )
        results = []
        for r in rows:
            msg = {
                "id": r["id"],
                "role": r["role"],
                "content": r["content"] or "",
                "created_at": str(r["created_at"]),
            }
            if r["role"] == "assistant":
                msg["data"] = {
                    "sql": r["generated_sql"] or "",
                    "explanation": r["explanation"] or "",
                    "message": r["friendly_message"] or "",
                    "intent": r["intent"] or "",
                    "execution_time_ms": r["execution_time_ms"] or 0,
                    "row_count": r["row_count"] or 0,
                }
                # Parse stored JSON result data
                if r["result_data"]:
                    try:
                        import json
                        msg["data"]["answer"] = json.loads(r["result_data"]) if isinstance(r["result_data"], str) else r["result_data"]
                    except (json.JSONDecodeError, TypeError):
                        msg["data"]["answer"] = []
                msg["_feedback"] = r["feedback"]
            results.append(msg)
        return results

    def save_user_message(self, conversation_id: str, content: str) -> str:
        """Save a user message and return its ID."""
        msg_id = str(uuid.uuid4())[:36]
        self.db_pool._execute_write_internal(
            """INSERT INTO messages (id, conversation_id, role, content)
               VALUES (:p0, :p1, :p2, :p3)""",
            (msg_id, conversation_id, "user", content),
        )
        return msg_id

    def save_assistant_message(
        self,
        conversation_id: str,
        content: str,
        generated_sql: str = "",
        explanation: str = "",
        friendly_message: str = "",
        intent: str = "",
        execution_time_ms: float = 0,
        row_count: int = 0,
        result_data: Optional[list] = None,
    ) -> str:
        """Save an assistant response and return its ID."""
        import json
        msg_id = str(uuid.uuid4())[:36]
        result_json = json.dumps(result_data[:100] if result_data else [], default=str)
        self.db_pool._execute_write_internal(
            """INSERT INTO messages
               (id, conversation_id, role, content, generated_sql, explanation,
                friendly_message, intent, execution_time_ms, row_count, result_data)
               VALUES (:p0, :p1, :p2, :p3, :p4, :p5, :p6, :p7, :p8, :p9, :p10)""",
            (
                msg_id, conversation_id, "assistant", content,
                generated_sql, explanation, friendly_message,
                intent, execution_time_ms, row_count, result_json,
            ),
        )
        return msg_id

    def update_feedback(self, message_id: str, rating: str):
        """Update feedback rating on a message."""
        self.db_pool._execute_write_internal(
            "UPDATE messages SET feedback = :p0 WHERE id = :p1",
            (rating, message_id),
        )

    def get_conversation_context(self, conversation_id: str, limit: int = 6) -> list[dict]:
        """Get recent user/sql pairs for conversation context (used by SQL generation)."""
        rows = self.db_pool.execute_query(
            """SELECT role, content, generated_sql
               FROM messages
               WHERE conversation_id = :conv_id
               ORDER BY created_at DESC
               LIMIT :lim""",
            {"conv_id": conversation_id, "lim": limit * 2},
        )
        context = []
        for r in reversed(rows):
            if r["role"] == "user":
                context.append({"user": r["content"]})
            elif r["role"] == "assistant" and r["generated_sql"] and context:
                context[-1]["sql"] = r["generated_sql"]
        return context[-limit:]
