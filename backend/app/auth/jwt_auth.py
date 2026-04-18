"""
JWT Authentication Service — Token creation, verification, and password hashing.
Production-grade auth with bcrypt password hashing and configurable expiry.
"""

import bcrypt
import structlog
from datetime import datetime, timedelta, timezone
from typing import Optional
from jose import jwt, JWTError

logger = structlog.get_logger()


class AuthService:
    """
    JWT-based authentication service.
    Handles token creation, verification, and password hashing.
    """

    def __init__(self, secret_key: str, algorithm: str = "HS256", expiry_hours: int = 8):
        self.secret_key = secret_key
        self.algorithm = algorithm
        self.expiry_hours = expiry_hours

    # ── Password Hashing ─────────────────────────────────

    @staticmethod
    def hash_password(password: str) -> str:
        """Hash a plaintext password using bcrypt."""
        pwd_bytes = password.encode("utf-8")
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(pwd_bytes, salt).decode("utf-8")

    @staticmethod
    def verify_password(plain_password: str, hashed_password: str) -> bool:
        """Verify a plaintext password against a bcrypt hash."""
        return bcrypt.checkpw(
            plain_password.encode("utf-8"),
            hashed_password.encode("utf-8"),
        )

    # ── Token Management ─────────────────────────────────

    def create_access_token(
        self,
        user_id: str,
        username: str,
        role: str,
        tenant_id: str = "default",
        extra_claims: Optional[dict] = None,
    ) -> str:
        """
        Create a JWT access token.
        
        Payload includes:
        - sub: user_id
        - username: display name
        - role: admin | analyst | viewer
        - tenant_id: multi-tenant isolation
        - exp: expiration timestamp
        - iat: issued-at timestamp
        """
        now = datetime.now(timezone.utc)
        payload = {
            "sub": user_id,
            "username": username,
            "role": role,
            "tenant_id": tenant_id,
            "exp": now + timedelta(hours=self.expiry_hours),
            "iat": now,
            "type": "access",
        }

        if extra_claims:
            payload.update(extra_claims)

        token = jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
        logger.info("token_created", user_id=user_id, role=role, tenant_id=tenant_id)
        return token

    def verify_token(self, token: str) -> dict:
        """
        Verify and decode a JWT token.
        Raises JWTError if invalid or expired.
        """
        try:
            payload = jwt.decode(token, self.secret_key, algorithms=[self.algorithm])
            return payload
        except JWTError as e:
            logger.warning("token_verification_failed", error=str(e))
            raise

    def create_api_key_token(self, key_name: str, tenant_id: str, role: str = "analyst") -> str:
        """
        Create a long-lived API key token (90 days).
        Used for programmatic access.
        """
        now = datetime.now(timezone.utc)
        payload = {
            "sub": f"apikey:{key_name}",
            "username": key_name,
            "role": role,
            "tenant_id": tenant_id,
            "exp": now + timedelta(days=90),
            "iat": now,
            "type": "api_key",
        }
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
