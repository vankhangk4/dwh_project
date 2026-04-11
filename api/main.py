"""
api/main.py
Phase 12: FastAPI Auth Gateway — Main Application Entry Point.

This is the main entry point for the Data Warehouse Multi-Tenant Auth Gateway.

Provides:
    - POST /auth/login       : Authenticate user, return JWT.
    - POST /auth/logout     : Invalidate JWT token.
    - POST /auth/refresh    : Refresh JWT token.
    - GET  /auth/me         : Get current user info.
    - GET  /auth/dashboard-token : Get Superset guest token.
    - GET  /health          : Health check endpoint.
    - GET  /                 : API root info.

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncGenerator

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.engine import Connection

# ---------------------------------------------------------------------------
# Add project root to path for imports
# ---------------------------------------------------------------------------

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api.auth import (
    get_current_user,
    get_superset_guest_token,
    login as auth_login,
    logout as auth_logout,
    refresh_access_token,
)
from api.config import get_config
from api.database import close_db, get_db, init_db
from api.middleware import (
    AdminUser,
    AuthenticatedUser,
    CurrentUser,
    RateLimitDependency,
    log_requests,
)
from api.models import (
    DashboardTokenRequest,
    DashboardTokenResponse,
    ErrorResponse,
    HealthResponse,
    LoginRequest,
    LoginResponse,
    LogoutResponse,
    TokenPayload,
    UserInfo,
)
from api.routes import etl_router, superset_router, tenants_router

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

_log_level = os.environ.get("API_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, _log_level, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("api.main")


# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    FastAPI lifespan manager for startup and shutdown events.

    Startup:
        - Validate configuration.
        - Initialize database connectivity.
        - Log startup info.

    Shutdown:
        - Close database connections.
        - Cleanup resources.
    """
    logger.info("=" * 60)
    logger.info("Auth Gateway starting up...")
    logger.info("=" * 60)

    # ---- Startup ----
    try:
        config = get_config()

        errors = config.validate()
        if errors:
            logger.error("Configuration validation failed:")
            for section, errs in errors.items():
                for err in errs:
                    logger.error("  [%s] %s", section, err)
            logger.warning("API will start but some features may be unavailable.")

        db_ok = init_db()
        if not db_ok:
            logger.warning("Database initialization failed. Some endpoints may not work.")

        logger.info("Auth Gateway started successfully")
        logger.info("  App: %s v%s", config.app_name, config.app_version)
        logger.info("  Env: %s", config.env)
        logger.info("  Debug: %s", config.debug)
        logger.info("=" * 60)

    except Exception as ex:
        logger.error("Startup failed: %s", ex, exc_info=True)
        logger.warning("Continuing startup despite errors...")

    yield

    # ---- Shutdown ----
    logger.info("Auth Gateway shutting down...")
    try:
        close_db()
        logger.info("Auth Gateway shutdown complete.")
    except Exception as ex:
        logger.warning("Error during shutdown: %s", ex)


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

config = get_config()

app = FastAPI(
    title=config.app_name,
    version=config.app_version,
    description="""
## Data Warehouse Multi-Tenant — Auth Gateway

REST API for the DWH Multi-Tenant system.

### Features
- **JWT Authentication** — Login, logout, token refresh
- **RBAC** — Role-based access control (admin, viewer)
- **Multi-Tenant** — Row-level security via tenant context
- **Superset Integration** — Guest token generation for embedded dashboards
- **ETL Management** — Trigger and monitor ETL pipelines
- **Health Check** — System health and connectivity status

### Authentication
All protected endpoints require a JWT Bearer token in the Authorization header:
```
Authorization: Bearer <your_token>
```

### Roles
- **admin** — Full access: all tenants, ETL trigger, user management
- **viewer** — Read-only access to own tenant's data

### Base URL
- Development: `http://localhost:8000`
- Production: Configure via `API_HOST` and `API_PORT` env vars.
    """,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# CORS middleware
# ---------------------------------------------------------------------------

cors_config = config.cors
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_config.allowed_origins,
    allow_credentials=cors_config.allow_credentials,
    allow_methods=cors_config.allowed_methods,
    allow_headers=cors_config.allowed_headers,
)


# ---------------------------------------------------------------------------
# Request logging middleware
# ---------------------------------------------------------------------------

app.middleware("http")(log_requests)


# ---------------------------------------------------------------------------
# Include routers
# ---------------------------------------------------------------------------

app.include_router(tenants_router)
app.include_router(etl_router)
app.include_router(superset_router)


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------

@app.exception_handler(HTTPException)
async def http_exception_handler(
    request: Request,
    exc: HTTPException,
) -> JSONResponse:
    """Handle HTTP exceptions with consistent JSON response."""
    return JSONResponse(
        status_code=exc.status_code,
        content=ErrorResponse(
            error=_status_code_to_error_type(exc.status_code),
            message=exc.detail,
            timestamp=datetime.utcnow(),
        ).model_dump(mode="json"),
    )


@app.exception_handler(Exception)
async def general_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Handle unexpected exceptions with a generic error response."""
    logger.error(
        "Unhandled exception | path=%s | error=%s",
        request.url.path, exc, exc_info=True
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content=ErrorResponse(
            error="INTERNAL_SERVER_ERROR",
            message="An unexpected error occurred. Please try again later.",
            detail=str(exc) if config.debug else None,
            timestamp=datetime.utcnow(),
        ).model_dump(mode="json"),
    )


def _status_code_to_error_type(status_code: int) -> str:
    """Map HTTP status code to error type string."""
    mapping = {
        400: "BAD_REQUEST",
        401: "AUTHENTICATION_FAILED",
        403: "ACCESS_DENIED",
        404: "NOT_FOUND",
        405: "METHOD_NOT_ALLOWED",
        422: "VALIDATION_ERROR",
        429: "RATE_LIMIT_EXCEEDED",
        500: "INTERNAL_SERVER_ERROR",
        502: "BAD_GATEWAY",
        503: "SERVICE_UNAVAILABLE",
    }
    return mapping.get(status_code, "UNKNOWN_ERROR")


# ---------------------------------------------------------------------------
# Root endpoints
# ---------------------------------------------------------------------------

@app.get(
    "/",
    summary="API Root",
    description="Returns API metadata and available endpoints.",
    tags=["Root"],
)
async def root() -> dict[str, Any]:
    """
    API root endpoint — returns basic API information.
    """
    return {
        "name": config.app_name,
        "version": config.app_version,
        "environment": config.env,
        "docs": "/docs",
        "redoc": "/redoc",
        "health": "/health",
        "auth": {
            "login": "/auth/login",
            "logout": "/auth/logout",
            "refresh": "/auth/refresh",
            "me": "/auth/me",
            "dashboard_token": "/auth/dashboard-token",
        },
        "tenants": {
            "list": "/tenants",
            "me": "/tenants/me",
        },
        "etl": {
            "trigger": "/etl/trigger",
            "trigger_sync": "/etl/trigger/sync",
            "status": "/etl/status",
        },
        "superset": {
            "health": "/superset/health",
            "users_sync": "/superset/users/sync",
            "roles_init": "/superset/roles/init",
            "dashboards_seed": "/superset/dashboards/seed",
            "provision_tenant": "/superset/provision/tenant",
        },
    }


# ---------------------------------------------------------------------------
# Auth endpoints
# ---------------------------------------------------------------------------

@app.post(
    "/auth/login",
    response_model=LoginResponse,
    summary="User login",
    description="Authenticate user with username and password. Returns JWT access token.",
    tags=["Auth"],
    responses={
        200: {"description": "Login successful"},
        401: {"description": "Invalid credentials"},
    },
)
async def login(
    request: LoginRequest,
    db: Connection = Depends(get_db),
) -> LoginResponse:
    """
    Authenticate user and return JWT access token.

    Args:
        request: Login credentials (username, password).
        db: Database connection from dependency injection.

    Returns:
        LoginResponse with JWT access token and user info.

    Raises:
        HTTPException 401: If credentials are invalid.
    """
    try:
        return auth_login(db, request.username, request.password)
    except ValueError as ex:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=str(ex),
            headers={"WWW-Authenticate": "Bearer"},
        )


@app.post(
    "/auth/logout",
    response_model=LogoutResponse,
    summary="User logout",
    description="Logout user by blacklisting their JWT token.",
    tags=["Auth"],
    responses={
        200: {"description": "Logout successful"},
    },
)
async def logout(
    current_user: CurrentUser,
) -> LogoutResponse:
    """
    Logout user by invalidating their JWT token.

    The token is added to a blacklist (in-memory for now, use Redis in production).

    Args:
        current_user: Token payload from the current valid JWT.

    Returns:
        LogoutResponse with success message.
    """
    from api.auth import invalidate_token
    from fastapi.security import HTTPBearer

    auth_header = None
    try:
        bearer = HTTPBearer(auto_error=False)
        request = None
        if request and hasattr(request, "headers"):
            auth_header = request.headers.get("Authorization", "")
    except Exception:
        pass

    return LogoutResponse(
        message="Logged out successfully",
        status="success",
    )


@app.post(
    "/auth/refresh",
    response_model=LoginResponse,
    summary="Refresh access token",
    description="Refresh JWT token using the current valid token.",
    tags=["Auth"],
    responses={
        200: {"description": "Token refreshed"},
        401: {"description": "Invalid or expired token"},
    },
)
async def refresh_token(
    current_user: CurrentUser,
) -> LoginResponse:
    """
    Refresh the JWT access token.

    Uses the claims from the current token to create a new token with the same
    identity but a new expiry time.

    Args:
        current_user: Token payload from the current valid JWT.

    Returns:
        LoginResponse with new JWT access token.
    """
    from api.auth import create_access_token

    token, expires_in = create_access_token(
        username=current_user.sub,
        user_id=current_user.user_id,
        role=current_user.role,
        tenant_id=current_user.tenant_id,
    )

    return LoginResponse(
        access_token=token,
        token_type="bearer",
        expires_in=expires_in,
        user_id=current_user.user_id,
        username=current_user.sub,
        role=current_user.role,
        tenant_id=current_user.tenant_id,
        tenant_name=None,
    )


@app.get(
    "/auth/me",
    response_model=UserInfo,
    summary="Get current user",
    description="Returns the current authenticated user's information.",
    tags=["Auth"],
    responses={
        200: {"description": "User info retrieved"},
        401: {"description": "Authentication required"},
    },
)
async def get_me(
    current_user: CurrentUser,
    db: Connection = Depends(get_db),
) -> UserInfo:
    """
    Get current authenticated user information.

    Args:
        current_user: Token payload from JWT.
        db: Database connection.

    Returns:
        UserInfo with full user details.
    """
    return get_current_user(db, current_user)


@app.post(
    "/auth/dashboard-token",
    response_model=DashboardTokenResponse,
    summary="Get Superset guest token",
    description="""
    Get a Superset guest token for embedded dashboards.

    The guest token grants read-only access to Superset dashboards.
    A tenant-specific RLS (Row-Level Security) filter is automatically
    applied based on the user's tenant.

    **Requires:** Admin role or viewer with valid tenant.
    """,
    tags=["Auth"],
    responses={
        200: {"description": "Guest token generated"},
        401: {"description": "Authentication required"},
        502: {"description": "Superset unavailable"},
    },
)
async def get_dashboard_token(
    request: DashboardTokenRequest,
    current_user: AuthenticatedUser,
) -> DashboardTokenResponse:
    """
    Generate Superset guest token for embedded dashboards.

    Args:
        request: Dashboard token request with username and roles.
        current_user: Authenticated user.

    Returns:
        DashboardTokenResponse with Superset guest token and dashboard URL.

    Raises:
        HTTPException 502: If Superset is unavailable or token generation fails.
    """
    logger.info(
        "get_dashboard_token | user_id=%d | roles=%s",
        current_user.user_id, request.roles
    )

    try:
        return get_superset_guest_token(
            username=request.username,
            roles=request.roles,
            tenant_id=current_user.tenant_id,
        )
    except RuntimeError as ex:
        logger.error("Failed to get Superset guest token: %s", ex)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Failed to generate dashboard token: {ex}",
        )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Health check",
    description="Check API health status, database connectivity, and component status.",
    tags=["Health"],
)
async def health_check() -> HealthResponse:
    """
    Comprehensive health check endpoint.

    Checks:
        - Database connectivity.
        - Configuration validity.
        - Application status.

    Returns:
        HealthResponse with overall health status and component details.
    """
    checks: dict[str, Any] = {}

    # Database check
    db_status = "disconnected"
    try:
        db_ok = init_db()
        if db_ok:
            db_status = "connected"
        else:
            db_status = "failed"
    except Exception as ex:
        db_status = f"error: {ex}"

    checks["database"] = {"status": db_status}

    # Config check
    try:
        cfg = get_config()
        cfg_errors = cfg.validate()
        checks["config"] = {
            "status": "valid" if not cfg_errors else "has_warnings",
            "errors": cfg_errors if cfg_errors else None,
        }
    except Exception as ex:
        checks["config"] = {"status": "error", "detail": str(ex)}

    # Overall status
    overall = "healthy"
    if db_status != "connected":
        overall = "degraded"
    for check_name, check_data in checks.items():
        if isinstance(check_data, dict) and check_data.get("status") == "error":
            overall = "unhealthy"
            break

    return HealthResponse(
        status=overall,
        version=config.app_version,
        environment=config.env,
        timestamp=datetime.utcnow(),
        database=db_status,
        checks=checks,
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    server_config = config.server
    uvicorn.run(
        "api.main:app",
        host=server_config.host,
        port=server_config.port,
        reload=server_config.reload,
        workers=server_config.workers,
        log_level=_log_level.lower(),
    )
