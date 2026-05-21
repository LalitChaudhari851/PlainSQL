"""
User Repository — MySQL-backed user persistence.

Replaces the in-memory dict user store that was wiped on every restart.
Uses the existing DatabasePool for connection management.
"""

import structlog
from typing import Optional

logger = structlog.get_logger()


class UserRepository:
    """
    Persistent user storage backed by MySQL.
    Provides CRUD operations for user management.
    """

    def __init__(self, db_pool):
        self.db_pool = db_pool
        self._ensure_table()

    def _ensure_table(self):
        """Create the users table if it doesn't exist."""
        try:
            self.db_pool._execute_write_internal("""
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
            logger.info("users_table_ready")
        except Exception as e:
            logger.warning("users_table_migration_failed", error=str(e))

    def get_by_username(self, username: str) -> Optional[dict]:
        """Retrieve a user by username."""
        try:
            rows = self.db_pool.execute_query(
                "SELECT id, username, email, password_hash, role, tenant_id, is_active "
                "FROM plainsql_users WHERE username = :username AND is_active = TRUE",
                {"username": username},
            )
            return rows[0] if rows else None
        except Exception as e:
            logger.error("user_lookup_failed", username=username, error=str(e))
            return None

    def get_by_id(self, user_id: str) -> Optional[dict]:
        """Retrieve a user by ID."""
        try:
            rows = self.db_pool.execute_query(
                "SELECT id, username, email, password_hash, role, tenant_id, is_active "
                "FROM plainsql_users WHERE id = :user_id AND is_active = TRUE",
                {"user_id": user_id},
            )
            return rows[0] if rows else None
        except Exception as e:
            logger.error("user_lookup_by_id_failed", user_id=user_id, error=str(e))
            return None

    def create(
        self,
        user_id: str,
        username: str,
        email: str,
        password_hash: str,
        role: str = "analyst",
        tenant_id: str = "default",
    ) -> bool:
        """
        Create a new user. Returns True on success, False if user already exists.
        Uses INSERT IGNORE to handle race conditions gracefully.
        """
        try:
            self.db_pool._execute_write_internal(
                """INSERT IGNORE INTO plainsql_users
                   (id, username, email, password_hash, role, tenant_id)
                   VALUES (:p0, :p1, :p2, :p3, :p4, :p5)""",
                (user_id, username, email, password_hash, role, tenant_id),
            )
            logger.info("user_created", username=username, role=role)
            return True
        except Exception as e:
            logger.error("user_creation_failed", username=username, error=str(e))
            return False

    def update_password(self, username: str, new_password_hash: str) -> bool:
        """Update a user's password hash."""
        try:
            self.db_pool._execute_write_internal(
                "UPDATE plainsql_users SET password_hash = :p0 WHERE username = :p1",
                (new_password_hash, username),
            )
            logger.info("password_updated", username=username)
            return True
        except Exception as e:
            logger.error("password_update_failed", username=username, error=str(e))
            return False

    def list_users(self, tenant_id: str = "default") -> list[dict]:
        """List all active users for a tenant (without password hashes)."""
        try:
            rows = self.db_pool.execute_query(
                "SELECT id, username, email, role, tenant_id, created_at "
                "FROM plainsql_users WHERE tenant_id = :tenant_id AND is_active = TRUE",
                {"tenant_id": tenant_id},
            )
            return rows
        except Exception as e:
            logger.error("user_list_failed", tenant_id=tenant_id, error=str(e))
            return []

    def deactivate(self, username: str) -> bool:
        """Soft-delete a user by marking as inactive."""
        try:
            self.db_pool._execute_write_internal(
                "UPDATE plainsql_users SET is_active = FALSE WHERE username = :p0",
                (username,),
            )
            logger.info("user_deactivated", username=username)
            return True
        except Exception as e:
            logger.error("user_deactivation_failed", username=username, error=str(e))
            return False

    def count(self) -> int:
        """Count active users."""
        try:
            rows = self.db_pool.execute_query(
                "SELECT COUNT(*) as cnt FROM plainsql_users WHERE is_active = TRUE"
            )
            return rows[0]["cnt"] if rows else 0
        except Exception:
            return 0
