"""
api/auth.py
Phase 12: Authentication and authorization for Auth Gateway.

Provides:
    - login()             : Authenticate user, return JWT access token.
    - verify_password()    : Verify bcrypt password hash.
    - create_access_token(): Create JWT token with claims.
    - decode_token()      : Decode and validate JWT token.
    - get_superset_guest_token() : Get Superset guest token for embedded dashboards.
    - logout()            : Invalidate token (blacklist).

Security:
    - JWT tokens signed with HS256 using a strong secret key.
    - Passwords verified with bcrypt.
    - Token expiry enforced.
    - Role-based access control (RBAC).

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
import os
import sys

# Ensure venv site-packages is at the front of sys.path.
# When running via `python -c "..."` or `python script.py`, sys.path[0]=""
# gets prepended automatically. We force venv site-packages to index 0
# so that venv packages (jwt, bcrypt, etc.) are found before any
# accidentally-shadowed names in the project directory.
_PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_VENV_SITE = os.path.join(_PROJ_ROOT, ".venv", "lib", "python3.13", "site-packages")
if _VENV_SITE not in sys.path:
    sys.path.insert(0, _VENV_SITE)
else:
    # Move venv_site to front if it's buried elsewhere
    sys.path.remove(_VENV_SITE)
    sys.path.insert(0, _VENV_SITE)

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import bcrypt
import jwt as _jwt
import requests
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.config import get_config
from api.models import (
    DashboardTokenResponse,
    LoginResponse,
    TokenPayload,
    UserInfo,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Token blacklist (in-memory; use Redis in production)
# ---------------------------------------------------------------------------

_token_blacklist: set[str] = set()
_blacklist_lock_instance: bool = False  # Placeholder for future Redis lock


def _is_token_blacklisted(jti: str) -> bool:
    """Check if a token JTI is in the blacklist."""
    return jti in _token_blacklist


def _blacklist_token(jti: str) -> None:
    """Add a token JTI to the blacklist."""
    _token_blacklist.add(jti)
    logger.info("Token blacklisted: jti=%s", jti[:8])


# ---------------------------------------------------------------------------
# Password hashing
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    """
    Hash a password using bcrypt.

    Args:
        password: Plain-text password.

    Returns:
        bcrypt hash string.
    """
    salt = bcrypt.gensalt(rounds=12)
    hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
    return hashed.decode("utf-8")


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify a plain-text password against a bcrypt hash.

    Args:
        plain_password:  Plain-text password to verify.
        hashed_password: bcrypt hash from database.

    Returns:
        True if password matches, False otherwise.
    """
    try:
        return bcrypt.checkpw(
            plain_password.encode("utf-8"),
            hashed_password.encode("utf-8"),
        )
    except Exception as ex:
        logger.error("Password verification error: %s", ex)
        return False


# ---------------------------------------------------------------------------
# JWT token management
# ---------------------------------------------------------------------------

def create_access_token(
    username: str,
    user_id: int,
    role: str,
    tenant_id: Optional[str] = None,
    expires_delta: Optional[timedelta] = None,
) -> tuple[str, int]:
    """
    Create a JWT access token.

    Args:
        username:      Username (sub claim).
        user_id:       User ID.
        role:          User role (admin/viewer).
        tenant_id:     Tenant ID (optional).
        expires_delta: Custom expiry delta. (optional)

    Returns:
        Tuple of (token string, expires_in seconds).
    """
    config = get_config()

    if expires_delta is None:
        expires_delta = timedelta(minutes=config.jwt.access_token_expire_minutes)

    now = datetime.now(timezone.utc)
    expire = now + expires_delta
    expires_in = int(expires_delta.total_seconds())

    payload = {
        "sub": username,
        "user_id": user_id,
        "role": role,
        "tenant_id": tenant_id,
        "iat": int(now.timestamp()),
        "exp": int(expire.timestamp()),
        "jti": f"{user_id}_{int(now.timestamp() * 1000)}",
        "type": "access",
    }

    token = _jwt.encode(
        payload,
        config.jwt.secret_key,
        algorithm=config.jwt.algorithm,
    )

    logger.info(
        "Access token created | user_id=%d | role=%s | tenant=%s | expires_in=%ds",
        user_id, role, tenant_id or "N/A", expires_in
    )

    return token, expires_in


def decode_token(token: str) -> TokenPayload:
    """
    Decode and validate a JWT access token.

    Args:
        token: JWT token string.

    Returns:
        TokenPayload with decoded claims.

    Raises:
        _jwt.ExpiredSignatureError: If token has expired.
        _jwt.InvalidTokenError: If token is invalid.
    """
    config = get_config()

    payload = _jwt.decode(
        token,
        config.jwt.secret_key,
        algorithms=[config.jwt.algorithm],
        options={"verify_exp": True},
    )

    # Check blacklist
    jti = payload.get("jti", "")
    if jti and _is_token_blacklisted(jti):
        raise _jwt.InvalidTokenError("Token has been revoked")

    return TokenPayload(
        sub=payload["sub"],
        user_id=payload["user_id"],
        role=payload["role"],
        tenant_id=payload.get("tenant_id"),
        exp=payload.get("exp"),
        iat=payload.get("iat"),
    )


def refresh_access_token(
    token: str,
) -> tuple[str, int]:
    """
    Refresh an access token using its claims.

    Args:
        token: Current valid JWT token.

    Returns:
        Tuple of (new token, expires_in seconds).

    Raises:
        _jwt.InvalidTokenError: If current token is invalid or blacklisted.
    """
    payload = decode_token(token)

    return create_access_token(
        username=payload.sub,
        user_id=payload.user_id,
        role=payload.role,
        tenant_id=payload.tenant_id,
    )


def invalidate_token(token: str) -> bool:
    """
    Invalidate (blacklist) a token.

    Args:
        token: JWT token to invalidate.

    Returns:
        True if successfully blacklisted, False otherwise.
    """
    try:
        payload = _jwt.decode(
            token,
            get_config().jwt.secret_key,
            algorithms=[get_config().jwt.algorithm],
            options={"verify_exp": False},
        )
        jti = payload.get("jti")
        if jti:
            _blacklist_token(jti)
            return True
        return False
    except Exception as ex:
        logger.error("Failed to invalidate token: %s", ex)
        return False


# ---------------------------------------------------------------------------
# User authentication
# ---------------------------------------------------------------------------

def authenticate_user(
    conn: Connection,
    username: str,
    password: str,
) -> Optional[dict[str, Any]]:
    """
    Authenticate a user against the AppUsers table.

    Args:
        conn:     SQLAlchemy database connection.
        username: Username to authenticate.
        password: Plain-text password.

    Returns:
        User dict if authentication succeeds, None otherwise.

    User dict keys:
        - UserID, Username, PasswordHash, Role, TenantID, IsActive, CreatedAt
        - TenantName (joined from Tenants table)
    """
    logger.debug("Authenticating user: %s", username)

    try:
        query = text("""
            SELECT
                u.UserID,
                u.Username,
                u.PasswordHash,
                u.Role,
                u.TenantID,
                u.IsActive,
                u.CreatedAt,
                t.TenantName
            FROM AppUsers u
            LEFT JOIN Tenants t ON u.TenantID = t.TenantID
            WHERE u.Username = :username
        """)

        result = conn.execute(query, {"username": username})
        conn.commit()
        row = result.fetchone()

        if row is None:
            logger.warning("Authentication failed: user not found: %s", username)
            return None

        user = {
            "UserID": row[0],
            "Username": row[1],
            "PasswordHash": row[2],
            "Role": row[3],
            "TenantID": row[4],
            "IsActive": row[5],
            "CreatedAt": row[6],
            "TenantName": row[7],
        }

        if not user["IsActive"]:
            logger.warning("Authentication failed: user inactive: %s", username)
            return None

        if not verify_password(password, user["PasswordHash"]):
            logger.warning("Authentication failed: wrong password: %s", username)
            return None

        logger.info(
            "Authentication successful | user_id=%d | role=%s | tenant=%s",
            user["UserID"], user["Role"], user["TenantID"] or "N/A"
        )
        return user

    except Exception as ex:
        logger.error("Authentication error for user %s: %s", username, ex)
        conn.rollback()
        return None


def login(
    conn: Connection,
    username: str,
    password: str,
) -> LoginResponse:
    """
    Perform user login and return JWT access token.

    Args:
        conn:     SQLAlchemy database connection.
        username: Username.
        password: Plain-text password.

    Returns:
        LoginResponse with JWT token and user info.

    Raises:
        ValueError: If credentials are invalid.
    """
    user = authenticate_user(conn, username, password)

    if user is None:
        raise ValueError("Invalid username or password")

    token, expires_in = create_access_token(
        username=user["Username"],
        user_id=user["UserID"],
        role=user["Role"],
        tenant_id=user["TenantID"],
    )

    return LoginResponse(
        access_token=token,
        token_type="bearer",
        expires_in=expires_in,
        user_id=user["UserID"],
        username=user["Username"],
        role=user["Role"],
        tenant_id=user["TenantID"],
        tenant_name=user["TenantName"],
    )


def get_current_user(
    conn: Connection,
    token_payload: TokenPayload,
) -> UserInfo:
    """
    Get current user information from token payload.

    Args:
        conn:          SQLAlchemy database connection.
        token_payload: Decoded JWT token payload.

    Returns:
        UserInfo with full user details.
    """
    try:
        query = text("""
            SELECT
                u.UserID,
                u.Username,
                u.Role,
                u.TenantID,
                u.IsActive,
                u.CreatedAt,
                t.TenantName
            FROM AppUsers u
            LEFT JOIN Tenants t ON u.TenantID = t.TenantID
            WHERE u.UserID = :user_id
        """)

        result = conn.execute(query, {"user_id": token_payload.user_id})
        conn.commit()
        row = result.fetchone()

        if row is None:
            raise ValueError("User not found")

        return UserInfo(
            user_id=row[0],
            username=row[1],
            role=row[2],
            tenant_id=row[3],
            tenant_name=row[7] if len(row) > 7 else None,
            is_active=row[5],
            created_at=row[6],
        )

    except Exception as ex:
        logger.error("Error fetching current user: %s", ex)
        raise ValueError(f"Failed to fetch user info: {ex}") from ex


def logout(token: str) -> bool:
    """
    Logout user by blacklisting their token.

    Args:
        token: JWT token to invalidate.

    Returns:
        True if successful, False otherwise.
    """
    return invalidate_token(token)


# ---------------------------------------------------------------------------
# Superset guest token
# ---------------------------------------------------------------------------

def get_superset_guest_token(
    username: str,
    roles: list[str],
    tenant_id: Optional[str] = None,
) -> DashboardTokenResponse:
    """
    Get a Superset guest token for embedded dashboards.

    This function:
    1. Authenticates with Superset API using admin credentials.
    2. Creates a guest token for the specified user/roles.
    3. Returns the token along with the dashboard URL.

    Args:
        username:  Username to impersonate (must exist in Superset).
        roles:     Superset roles to grant (e.g. ['Gamma']).
        tenant_id: Tenant ID for RBAC filtering. (optional)

    Returns:
        DashboardTokenResponse with token and URL.

    Raises:
        RuntimeError: If Superset authentication or token creation fails.
    """
    config = get_config()

    superset_url = config.superset.url.rstrip("/")
    admin_username = config.superset.username
    admin_password = config.superset.password

    if not admin_password:
        raise RuntimeError(
            "SUPERSET_PASSWORD is not configured. "
            "Cannot generate guest token."
        )

    # Step 1: Get access token from Superset
    token_url = f"{superset_url}/api/v1/security/login"
    login_payload = {
        "username": admin_username,
        "password": admin_password,
        "provider": "db",
        "refresh": True,
    }

    try:
        login_response = requests.post(
            token_url,
            json=login_payload,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )

        if login_response.status_code != 200:
            logger.error(
                "Superset login failed: status=%d, body=%s",
                login_response.status_code, login_response.text
            )
            raise RuntimeError(
                f"Superset login failed: HTTP {login_response.status_code}"
            )

        login_data = login_response.json()
        access_token = login_data.get("access_token")
        refresh_token = login_data.get("refresh_token")

        if not access_token:
            raise RuntimeError("No access token returned from Superset")

        logger.debug("Superset admin login successful")

    except requests.RequestException as ex:
        logger.error("Failed to connect to Superset: %s", ex)
        raise RuntimeError(f"Failed to connect to Superset: {ex}") from ex

    # Step 2: Create guest token
    guest_url = f"{superset_url}/api/v1/security/guest_token"
    guest_payload: dict[str, Any] = {
        "user": {
            "username": username,
            "roles": roles,
        },
        "resources": [
            {"type": "dashboard", "id": "1"},
        ],
        "rls": [],
    }

    # Add RLS filter for tenant if applicable
    if tenant_id:
        guest_payload["rls"] = [
            {"clause": f"TenantID = '{tenant_id}'"},
        ]

    try:
        guest_response = requests.post(
            guest_url,
            json=guest_payload,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            timeout=30,
        )

        if guest_response.status_code not in (200, 201):
            logger.error(
                "Guest token creation failed: status=%d, body=%s",
                guest_response.status_code, guest_response.text
            )
            raise RuntimeError(
                f"Guest token creation failed: HTTP {guest_response.status_code}"
            )

        guest_data = guest_response.json()
        token = guest_data.get("token")
        expires_in = config.superset.guest_token_expiry_seconds

        if not token:
            raise RuntimeError("No guest token returned from Superset")

        logger.info(
            "Superset guest token created | user=%s | roles=%s | tenant=%s",
            username, roles, tenant_id or "N/A"
        )

    except requests.RequestException as ex:
        logger.error("Failed to create Superset guest token: %s", ex)
        raise RuntimeError(f"Failed to create guest token: {ex}") from ex

    # Step 3: Build dashboard URL
    dashboard_url = f"{superset_url}/superset/dashboard/1/?guest_token={token}"

    return DashboardTokenResponse(
        token=token,
        token_type="bearer",
        expires_in=expires_in,
        dashboard_url=dashboard_url,
    )
