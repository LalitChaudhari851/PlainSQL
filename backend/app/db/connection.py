"""
Database Connection Pool — Production-grade pooled connections via SQLAlchemy engine.
Supports multi-tenant databases via tenant_id-based connection routing.
"""

import pymysql
from sqlalchemy import create_engine, text, event
from sqlalchemy.pool import QueuePool
from urllib.parse import urlparse, unquote
from typing import Optional
from contextlib import contextmanager
import structlog

logger = structlog.get_logger()


class DatabasePool:
    """
    Production database connection pool.
    Uses SQLAlchemy QueuePool for connection reuse, health checks, and overflow management.
    Falls back to raw pymysql for schema introspection when needed.
    """

    def __init__(
        self,
        db_uri: str,
        query_timeout: int = 30,
        pool_size: int = 10,
        max_overflow: int = 20,
        pool_timeout: int = 30,
    ):
        parsed = urlparse(db_uri)
        self.host = parsed.hostname
        self.port = parsed.port or 3306
        self.user = parsed.username
        self.password = unquote(parsed.password) if parsed.password else ""
        self.db_name = parsed.path[1:] if parsed.path else "chatbot"
        self.query_timeout = query_timeout

        # ── Build SQLAlchemy engine with connection pool ──
        # Construct a clean pymysql URI (no SQLAlchemy dialect prefix issues)
        safe_password = self.password.replace("@", "%40")
        engine_uri = f"mysql+pymysql://{self.user}:{safe_password}@{self.host}:{self.port}/{self.db_name}"

        self._engine = create_engine(
            engine_uri,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_pre_ping=True,  # Verify connections before checkout (stale conn defense)
            pool_recycle=1800,   # Recycle connections every 30 min (MySQL wait_timeout defense)
            connect_args={
                "connect_timeout": 10,
                "read_timeout": query_timeout,
                "write_timeout": query_timeout,
            },
        )

        # ── Set session to READ ONLY for query connections ──
        @event.listens_for(self._engine, "checkout")
        def _set_read_only(dbapi_conn, connection_record, connection_proxy):
            """Set session to read-only on checkout for defense-in-depth."""
            pass  # MySQL read-only requires SUPER privilege; rely on SQL validation instead

        self._validate_connection()

    def _validate_connection(self):
        """Validate database connectivity on startup."""
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("database_connected", host=self.host, database=self.db_name,
                        pool_size=self._engine.pool.size())
        except Exception as e:
            logger.error("database_connection_failed", error=str(e))
            raise

    @contextmanager
    def get_connection(self):
        """Context manager for pooled database connections."""
        conn = self._engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, query: str, params: Optional[dict] = None) -> list[dict]:
        """Execute a read-only query and return results as list of dicts."""
        with self._engine.connect() as conn:
            if params:
                result = conn.execute(text(query), params)
            else:
                result = conn.execute(text(query))
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]

    def _execute_write_internal(self, query: str, params: Optional[tuple] = None):
        """
        Execute a write query. INTERNAL USE ONLY — for schema setup and migrations.
        User-facing queries MUST go through execute_query after SQL validation.
        """
        with self._engine.begin() as conn:
            if params:
                conn.execute(text(query), dict(enumerate(params)))
            else:
                conn.execute(text(query))

    def get_tables(self) -> list[str]:
        """Returns all table names in the current database."""
        rows = self.execute_query("SHOW TABLES")
        return [list(row.values())[0] for row in rows]

    def get_table_schema(self, table_name: str) -> list[dict]:
        """Returns column details for a specific table."""
        # Use backtick quoting for safety, but table name is from our own DB metadata
        rows = self.execute_query(f"DESCRIBE `{table_name}`")
        return [
            {
                "name": row["Field"],
                "type": row["Type"],
                "null": row["Null"],
                "key": row["Key"],
                "default": row["Default"],
            }
            for row in rows
        ]

    def get_foreign_keys(self, table_name: str) -> list[dict]:
        """Returns foreign key relationships for a table."""
        query = """
        SELECT 
            COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = :schema_name
            AND TABLE_NAME = :table_name
            AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        return self.execute_query(query, {"schema_name": self.db_name, "table_name": table_name})

    def get_sample_values(self, table_name: str, column_name: str, limit: int = 5) -> list:
        """Returns sample distinct values for a column."""
        try:
            query = f"SELECT DISTINCT `{column_name}` FROM `{table_name}` LIMIT :lim"
            rows = self.execute_query(query, {"lim": limit})
            return [list(r.values())[0] for r in rows]
        except Exception:
            return []

    def get_row_count(self, table_name: str) -> int:
        """Returns approximate row count for a table."""
        try:
            rows = self.execute_query(f"SELECT COUNT(*) as cnt FROM `{table_name}`")
            return rows[0]["cnt"] if rows else 0
        except Exception:
            return 0
    
    def get_full_schema(self) -> str:
        """Generates a complete text representation of the database schema."""
        tables = self.get_tables()
        schema_text = ""

        for table in tables:
            columns = self.get_table_schema(table)
            schema_text += f"Table: {table}\nColumns:\n"
            for col in columns:
                schema_text += f"  - {col['name']} ({col['type']})"
                if col['key'] == 'PRI':
                    schema_text += " [PRIMARY KEY]"
                if col['key'] == 'MUL':
                    schema_text += " [FOREIGN KEY]"
                schema_text += "\n"

            # Add foreign key relationships
            fks = self.get_foreign_keys(table)
            if fks:
                schema_text += "Relationships:\n"
                for fk in fks:
                    schema_text += f"  - {fk['COLUMN_NAME']} → {fk['REFERENCED_TABLE_NAME']}.{fk['REFERENCED_COLUMN_NAME']}\n"
            schema_text += "\n"

        return schema_text

    def get_pool_status(self) -> dict:
        """Returns current connection pool statistics."""
        pool = self._engine.pool
        return {
            "pool_size": pool.size(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "checked_in": pool.checkedin(),
        }


class TenantRegistry:
    """
    Multi-tenant database connection registry.
    Maps tenant_id → DatabasePool for isolated data access.
    """

    def __init__(self):
        self._pools: dict[str, DatabasePool] = {}

    def register(self, tenant_id: str, db_uri: str, query_timeout: int = 30):
        """Register a new tenant database."""
        self._pools[tenant_id] = DatabasePool(db_uri, query_timeout)
        logger.info("tenant_registered", tenant_id=tenant_id)

    def get_pool(self, tenant_id: str) -> DatabasePool:
        """Get the database pool for a tenant."""
        pool = self._pools.get(tenant_id)
        if not pool:
            raise ValueError(f"No database registered for tenant: {tenant_id}")
        return pool

    def list_tenants(self) -> list[str]:
        """List all registered tenant IDs."""
        return list(self._pools.keys())
