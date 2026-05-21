"""Create plainsql_users and query_feedback tables

Revision ID: 001
Revises: None
Create Date: 2026-05-02
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── Users table (replaces in-memory user store) ──────
    op.execute("""
        CREATE TABLE IF NOT EXISTS plainsql_users (
            id VARCHAR(64) PRIMARY KEY,
            username VARCHAR(100) NOT NULL UNIQUE,
            email VARCHAR(255) NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            role ENUM('admin', 'analyst', 'viewer') NOT NULL DEFAULT 'analyst',
            tenant_id VARCHAR(100) NOT NULL DEFAULT 'default',
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_username (username),
            INDEX idx_tenant (tenant_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)

    # ── Query feedback table (user satisfaction tracking) ─
    op.execute("""
        CREATE TABLE IF NOT EXISTS query_feedback (
            id INT AUTO_INCREMENT PRIMARY KEY,
            trace_id VARCHAR(100) NOT NULL,
            user_query TEXT NOT NULL,
            generated_sql TEXT,
            rating ENUM('positive', 'negative') NOT NULL,
            comment TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            tenant_id VARCHAR(100) DEFAULT 'default',
            INDEX idx_trace (trace_id),
            INDEX idx_created (created_at)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS query_feedback")
    op.execute("DROP TABLE IF EXISTS plainsql_users")
