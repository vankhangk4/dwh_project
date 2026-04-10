"""
api/middleware.py
Phase 12: JWT verification middleware for Auth Gateway.

Provides:
    - get_current_user_from_token() : FastAPI dependency — extract & verify JWT from header.
    - require_role()                : FastAPI dependency factory — enforce role check.
    - require_admin()              : FastAPI dependency — require admin role only.
    - require_viewer_or_admin()    : FastAPI dependency — require viewer or admin role.
    - TenantContext                : Dependency that sets tenant context on DB connection.
    - RateLimitMiddleware          : Simple in-memory rate limiting per IP/user.

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Annotated, Callable, Optional

import jwt as _jwt
import pyodbc
from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.auth import decode_token
from api.config import get_config
from api.models import TokenPayload

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Security scheme
# ---------------------------------------------------------------------------

bearer_scheme = HTTPBearer(
    scheme_name="JWT Bearer Token",
    description="Enter your JWT access token",
    auto_error=True,
)


# ---------------------------------------------------------------------------
# JWT verification dependency
# ---------------------------------------------------------------------------

def get_current_user_from_token(
    credentials: Annotated[HTTPAuthorizationCredentials, Security(bearer_scheme)],
) -> TokenPayload:
    """
    FastAPI dependency that verifies JWT token and returns decoded payload.

    This is the primary authentication dependency. All protected endpoints
    should use this to extract the current user from the Authorization header.

    Args:
        credentials: HTTP Bearer credentials extracted from request header.

    Returns:
        TokenPayload with decoded JWT claims.

    Raises:
        HTTPException 401: If token is invalid, expired, or blacklisted.
    """
    token = credentials.credentials

    try:
        payload = decode_token(token)
        return payload

    except _jwt.ExpiredSignatureError:
        logger.warning("Token expired")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired. Please login again.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    except _jwt.InvalidTokenError as ex:
        logger.warning("Invalid token: %s", ex)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or malformed token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    except Exception as ex:
        logger.error("Unexpected token verification error: %s", ex)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed.",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ---------------------------------------------------------------------------
# Role-based access control
# ---------------------------------------------------------------------------

def require_role(allowed_roles: list[str]) -> Callable:
    """
    FastAPI dependency factory that enforces role-based access control.

    Args:
        allowed_roles: List of roles that are allowed to access the endpoint.
                       Example: ["admin"] or ["admin", "viewer"]

    Returns:
        FastAPI dependency that raises HTTPException 403 if role is not allowed.

    Example:
        @app.get("/admin-only", dependencies=[RequireRole(["admin"])])
        def admin_endpoint(user: TokenPayload = Depends(get_current_user_from_token)):
            return {"message": "Welcome, admin!"}
    """
    def dependency(
        current_user: Annotated[TokenPayload, Depends(get_current_user_from_token)],
    ) -> TokenPayload:
        if current_user.role not in allowed_roles:
            logger.warning(
                "Access denied | user_id=%d | role=%s | required=%s",
                current_user.user_id, current_user.role, allowed_roles
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied. Required role: {allowed_roles}",
            )
        return current_user

    dependency.__name__ = f"require_role_{'_'.join(allowed_roles)}"
    return dependency


# Pre-built role dependencies
require_admin = require_role(["admin"])
require_viewer_or_admin = require_role(["admin", "viewer"])


# ---------------------------------------------------------------------------
# Tenant context injection
# ---------------------------------------------------------------------------

def inject_tenant_context(
    conn: Connection,
    tenant_id: Optional[str],
) -> None:
    """
    Set tenant context on the database connection using SESSION_CONTEXT.

    This enables row-level security in SQL Server by setting a session variable
    that stored procedures can read via SESSION_CONTEXT('TenantID').

    Args:
        conn:      SQLAlchemy connection.
        tenant_id: Tenant ID to set. If None, sets to 'NO_TENANT'.
    """
    effective_tenant = tenant_id or "NO_TENANT"

    try:
        conn.execute(
            text("SET SESSION_CONTEXT 'TenantID' = :tenant_id"),
            {"tenant_id": effective_tenant},
        )
        conn.commit()
        logger.debug("Tenant context set: %s", effective_tenant)
    except Exception as ex:
        logger.warning("Failed to set tenant context: %s", ex)


class TenantContext:
    """
    FastAPI dependency that sets tenant context on DB connection.

    Use as a parameter in route handlers that need tenant isolation.

    Usage:
        @app.get("/data")
        def get_data(
            db: Connection = Depends(get_db),
            ctx: TenantContext = Depends(TenantContext()),
        ):
            inject_tenant_context(db, ctx.tenant_id)
            ...
    """

    def __init__(
        self,
        required: bool = False,
    ):
        """
        Initialize TenantContext dependency.

        Args:
            required: If True, tenant_id must be present in token. (default False)
        """
        self.required = required

    def __call__(
        self,
        current_user: Annotated[TokenPayload, Depends(get_current_user_from_token)],
    ) -> TokenPayload:
        if self.required and not current_user.tenant_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Tenant ID is required for this endpoint.",
            )
        return current_user


# ---------------------------------------------------------------------------
# Rate limiting (simple in-memory — use Redis in production)
# ---------------------------------------------------------------------------

_rate_limit_store: dict[str, list[float]] = defaultdict(list)
_RATE_LIMIT_WINDOW_SECONDS = 60
_RATE_LIMIT_MAX_REQUESTS = 100


def _get_client_ip(request: Request) -> str:
    """Extract client IP address from request, handling proxies."""
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.headers.get("X-Real-IP"):
        return request.headers.get("X-Real-IP")
    return request.client.host if request.client else "unknown"


def _is_rate_limited(client_ip: str, max_requests: int = _RATE_LIMIT_MAX_REQUESTS) -> bool:
    """
    Check if client IP is rate limited.

    Args:
        client_ip:    Client IP address.
        max_requests: Maximum requests per window.

    Returns:
        True if rate limited, False otherwise.
    """
    now = time.time()
    window_start = now - _RATE_LIMIT_WINDOW_SECONDS

    timestamps = _rate_limit_store[client_ip]
    timestamps = [ts for ts in timestamps if ts > window_start]
    _rate_limit_store[client_ip] = timestamps

    if len(timestamps) >= max_requests:
        logger.warning("Rate limit exceeded for IP: %s", client_ip)
        return True

    timestamps.append(now)
    return False


class RateLimitDependency:
    """
    FastAPI dependency for rate limiting per client IP or user.

    Usage:
        @app.get("/data", dependencies=[RateLimitDependency(max_requests=50)])
        def get_data():
            ...
    """

    def __init__(
        self,
        max_requests: int = _RATE_LIMIT_MAX_REQUESTS,
        window_seconds: int = _RATE_LIMIT_WINDOW_SECONDS,
    ):
        self.max_requests = max_requests
        self.window_seconds = window_seconds

    def __call__(self, request: Request) -> None:
        client_ip = _get_client_ip(request)
        if _is_rate_limited(client_ip, self.max_requests):
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=(
                    f"Rate limit exceeded. Maximum {self.max_requests} requests "
                    f"per {self.window_seconds} seconds."
                ),
            )


# ---------------------------------------------------------------------------
# Request logging middleware
# ---------------------------------------------------------------------------

async def log_requests(request: Request, call_next):
    """
    ASGI middleware that logs all incoming HTTP requests.

    Logs: method, path, client IP, user agent, response status, duration.

    Usage (in main.py):
        app.middleware("http")(log_requests)
    """
    start_time = time.time()
    client_ip = _get_client_ip(request)
    method = request.method
    path = request.url.path

    logger.info(
        "Request started | method=%s | path=%s | client=%s",
        method, path, client_ip
    )

    try:
        response = await call_next(request)
    except Exception as ex:
        logger.error(
            "Request failed | method=%s | path=%s | error=%s",
            method, path, ex
        )
        raise

    duration_ms = int((time.time() - start_time) * 1000)
    logger.info(
        "Request completed | method=%s | path=%s | status=%d | duration=%dms | client=%s",
        method, path, response.status_code, duration_ms, client_ip
    )

    return response


# ---------------------------------------------------------------------------
# Global type aliases for dependency injection
# ---------------------------------------------------------------------------

CurrentUser = Annotated[TokenPayload, Depends(get_current_user_from_token)]
AdminUser = Annotated[TokenPayload, Depends(require_admin)]
AuthenticatedUser = Annotated[TokenPayload, Depends(require_viewer_or_admin)]
TenantScoped = Annotated[TokenPayload, Depends(TenantContext())]
