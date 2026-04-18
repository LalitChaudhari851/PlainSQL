"""
Tests for the RBAC and Auth system.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.auth.jwt_auth import AuthService
from app.auth.rbac import Role, Permission, check_permission, ROLE_PERMISSIONS


class TestAuthService:
    """Test JWT token creation and verification."""

    def setup_method(self):
        self.auth = AuthService(secret_key="test-secret-key-for-tests", expiry_hours=1)

    def test_create_and_verify_token(self):
        token = self.auth.create_access_token(
            user_id="user_1", username="testuser", role="analyst", tenant_id="default"
        )
        payload = self.auth.verify_token(token)
        assert payload["sub"] == "user_1"
        assert payload["username"] == "testuser"
        assert payload["role"] == "analyst"
        assert payload["tenant_id"] == "default"

    def test_password_hashing(self):
        password = "SecurePassword123!"
        hashed = self.auth.hash_password(password)
        assert self.auth.verify_password(password, hashed) is True
        assert self.auth.verify_password("WrongPassword", hashed) is False

    def test_api_key_creation(self):
        api_key = self.auth.create_api_key_token(
            key_name="test-key", tenant_id="default", role="analyst"
        )
        payload = self.auth.verify_token(api_key)
        assert payload["type"] == "api_key"
        assert payload["role"] == "analyst"


class TestRBAC:
    """Test Role-Based Access Control."""

    def test_admin_has_all_permissions(self):
        for perm in Permission:
            assert check_permission("admin", perm) is True

    def test_analyst_permissions(self):
        assert check_permission("analyst", Permission.READ) is True
        assert check_permission("analyst", Permission.EXECUTE) is True
        assert check_permission("analyst", Permission.VIEW_ANALYTICS) is True
        assert check_permission("analyst", Permission.MANAGE_USERS) is False
        assert check_permission("analyst", Permission.MANAGE_API_KEYS) is False

    def test_viewer_permissions(self):
        assert check_permission("viewer", Permission.READ) is True
        assert check_permission("viewer", Permission.EXECUTE) is False
        assert check_permission("viewer", Permission.MANAGE_USERS) is False

    def test_invalid_role(self):
        assert check_permission("hacker", Permission.READ) is False
