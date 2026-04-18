"""
Conversations API Router — CRUD for server-side chat persistence.

Provides endpoints for managing conversations and messages,
enabling cross-device chat history and user-bound data.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
import structlog

logger = structlog.get_logger()


def create_conversations_router(conversation_manager, auth_dep=None):
    """Factory function to create the conversations router with injected dependencies."""
    router = APIRouter(prefix="/api/v1/conversations", tags=["conversations"])

    class CreateConversationRequest(BaseModel):
        title: str = Field(default="New analysis", max_length=255)

    class UpdateTitleRequest(BaseModel):
        title: str = Field(..., min_length=1, max_length=255)

    # ── List conversations ───────────────────────────────
    @router.get("")
    async def list_conversations():
        """List all conversations for the current user."""
        try:
            conversations = conversation_manager.list_conversations(user_id="anonymous")
            return {"conversations": conversations}
        except Exception as e:
            logger.error("list_conversations_failed", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to load conversations")

    # ── Create conversation ──────────────────────────────
    @router.post("")
    async def create_conversation(request: CreateConversationRequest):
        """Create a new conversation."""
        try:
            conv = conversation_manager.create_conversation(
                title=request.title,
                user_id="anonymous",
            )
            return conv
        except Exception as e:
            logger.error("create_conversation_failed", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to create conversation")

    # ── Update conversation title ────────────────────────
    @router.patch("/{conversation_id}")
    async def update_conversation(conversation_id: str, request: UpdateTitleRequest):
        """Update a conversation's title."""
        try:
            conversation_manager.update_title(conversation_id, request.title)
            return {"status": "ok"}
        except Exception as e:
            logger.error("update_conversation_failed", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to update conversation")

    # ── Delete conversation ──────────────────────────────
    @router.delete("/{conversation_id}")
    async def delete_conversation(conversation_id: str):
        """Delete a conversation and all its messages."""
        try:
            conversation_manager.delete_conversation(conversation_id, user_id="anonymous")
            return {"status": "ok"}
        except Exception as e:
            logger.error("delete_conversation_failed", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to delete conversation")

    # ── Get messages ─────────────────────────────────────
    @router.get("/{conversation_id}/messages")
    async def get_messages(conversation_id: str):
        """Get all messages in a conversation."""
        try:
            messages = conversation_manager.get_messages(conversation_id)
            return {"messages": messages}
        except Exception as e:
            logger.error("get_messages_failed", error=str(e))
            raise HTTPException(status_code=500, detail="Failed to load messages")

    return router
