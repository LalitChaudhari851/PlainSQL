"""
Role-Based Access Control (RBAC) — Permission enforcement for API endpoints.
Three roles: admin, analyst, viewer with hierarchical permissions.
"""

from enum import Enum
from functools import wraps
from fastapi import HTTPException, status
import structlog

logger = structlog.get_logger()


class Role(str, Enum):
    """User roles with increasing privilege levels."""
    VIEWER = "viewer"       # Can view data only
    ANALYST = "analyst"     # Can execute queries and view analytics
    ADMIN = "admin"         # Full access including user management


class Permission(str, Enum):
    """Granular permissions mapped to roles."""
    READ = "read"                   # View query results
    EXECUTE = "execute"             # Execute SQL queries
    VIEW_ANALYTICS = "view_analytics"  # View usage analytics
    MANAGE_SCHEMA = "manage_schema"    # Refresh schema index
    MANAGE_USERS = "manage_users"      # Create/delete users
    MANAGE_API_KEYS = "manage_api_keys"  # Create API keys
    VIEW_LOGS = "view_logs"            # View system logs


# Role → Permission mapping
ROLE_PERMISSIONS: dict[Role, set[Permission]] = {
    Role.VIEWER: {
        Permission.READ,
    },
    Role.ANALYST: {
        Permission.READ,
        Permission.EXECUTE,
        Permission.VIEW_ANALYTICS,
    },
    Role.ADMIN: {
        Permission.READ,
        Permission.EXECUTE,
        Permission.VIEW_ANALYTICS,
        Permission.MANAGE_SCHEMA,
        Permission.MANAGE_USERS,
        Permission.MANAGE_API_KEYS,
        Permission.VIEW_LOGS,
    },
}


def check_permission(user_role: str, required_permission: Permission) -> bool:
    """Check if a role has a specific permission."""
    try:
        role = Role(user_role)
    except ValueError:
        return False
    return required_permission in ROLE_PERMISSIONS.get(role, set())


def require_role(minimum_role: Role):
    """
    FastAPI dependency that enforces minimum role level.
    Usage: Depends(require_role(Role.ANALYST))
    """
    role_hierarchy = {Role.VIEWER: 0, Role.ANALYST: 1, Role.ADMIN: 2}

    def checker(current_user: dict):
        user_role = current_user.get("role", "viewer")
        try:
            user_level = role_hierarchy.get(Role(user_role), -1)
            required_level = role_hierarchy.get(minimum_role, 99)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Invalid role: {user_role}",
            )

        if user_level < required_level:
            logger.warning(
                "access_denied",
                user_role=user_role,
                required_role=minimum_role.value,
                user=current_user.get("sub"),
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient permissions. Required: {minimum_role.value}, Current: {user_role}",
            )
        return current_user

    return checker
