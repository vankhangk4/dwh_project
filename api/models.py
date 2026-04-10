"""
api/models.py
Phase 12: Pydantic models for Auth Gateway.

Defines all request/response models used by the API endpoints.

Author: Nguyen Van Khang
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Auth models
# ---------------------------------------------------------------------------

class LoginRequest(BaseModel):
    """Request model for user login."""
    username: str = Field(..., min_length=1, max_length=100, description="Username")
    password: str = Field(..., min_length=1, description="Password")

    @field_validator("username", "password", mode="before")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        if isinstance(v, str):
            return v.strip()
        return v

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"username": "admin", "password": "StrongPassword123!"}
            ]
        }
    }


class LoginResponse(BaseModel):
    """Response model for successful login."""
    access_token: str = Field(..., description="JWT access token")
    token_type: str = Field(default="bearer", description="Token type")
    expires_in: int = Field(..., description="Token expiry in seconds")
    user_id: int = Field(..., description="User ID")
    username: str = Field(..., description="Username")
    role: str = Field(..., description="User role")
    tenant_id: Optional[str] = Field(None, description="Tenant ID (if applicable)")
    tenant_name: Optional[str] = Field(None, description="Tenant name")

    model_config = {
        "from_attributes": True,
        "json_schema_extra": {
            "examples": [
                {
                    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                    "token_type": "bearer",
                    "expires_in": 3600,
                    "user_id": 1,
                    "username": "admin",
                    "role": "admin",
                    "tenant_id": "STORE_HN",
                    "tenant_name": "Cửa Hàng Hà Nội",
                }
            ]
        }
    }


class TokenPayload(BaseModel):
    """JWT token payload model."""
    sub: str = Field(..., description="Subject (username)")
    user_id: int = Field(..., description="User ID")
    role: str = Field(..., description="User role")
    tenant_id: Optional[str] = Field(None, description="Tenant ID")
    exp: Optional[int] = Field(None, description="Expiration timestamp")
    iat: Optional[int] = Field(None, description="Issued at timestamp")

    model_config = {"from_attributes": True}


class UserInfo(BaseModel):
    """Current user information."""
    user_id: int = Field(..., description="User ID")
    username: str = Field(..., description="Username")
    role: str = Field(..., description="User role")
    tenant_id: Optional[str] = Field(None, description="Tenant ID")
    tenant_name: Optional[str] = Field(None, description="Tenant name")
    is_active: bool = Field(..., description="Account active status")
    created_at: Optional[datetime] = Field(None, description="Account creation time")

    model_config = {"from_attributes": True}


class LogoutResponse(BaseModel):
    """Response model for logout."""
    message: str = Field(default="Logged out successfully")
    status: str = Field(default="success")

    model_config = {
        "json_schema_extra": {
            "examples": [{"message": "Logged out successfully", "status": "success"}]
        }
    }


# ---------------------------------------------------------------------------
# Superset models
# ---------------------------------------------------------------------------

class DashboardTokenRequest(BaseModel):
    """Request model for Superset guest token."""
    username: str = Field(..., min_length=1, description="Username to impersonate")
    roles: list[str] = Field(
        default=["Gamma"],
        description="Superset roles to grant (e.g. ['Gamma', 'Alpha'])",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"username": "store_hn_viewer", "roles": ["Gamma"]}
            ]
        }
    }


class DashboardTokenResponse(BaseModel):
    """Response model for Superset guest token."""
    token: str = Field(..., description="Superset guest token (JWT)")
    token_type: str = Field(default="bearer", description="Token type")
    expires_in: int = Field(..., description="Token expiry in seconds")
    dashboard_url: str = Field(..., description="Superset dashboard URL")

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "token": "guest_token_abc123...",
                    "token_type": "bearer",
                    "expires_in": 3600,
                    "dashboard_url": "http://localhost:8088/superset/dashboard/1/?guest_token=guest_token_abc123",
                }
            ]
        }
    }


# ---------------------------------------------------------------------------
# Tenant models
# ---------------------------------------------------------------------------

class TenantInfo(BaseModel):
    """Tenant information model."""
    tenant_id: str = Field(..., description="Tenant ID")
    tenant_name: str = Field(..., description="Tenant display name")
    is_active: bool = Field(..., description="Tenant active status")
    created_at: Optional[datetime] = Field(None, description="Tenant creation time")

    model_config = {"from_attributes": True}


class TenantListResponse(BaseModel):
    """Response model for tenant list."""
    tenants: list[TenantInfo] = Field(..., description="List of tenants")
    total: int = Field(..., description="Total number of tenants")


# ---------------------------------------------------------------------------
# ETL models
# ---------------------------------------------------------------------------

class ETLTriggerRequest(BaseModel):
    """Request model for triggering ETL pipeline."""
    tenant_ids: Optional[list[str]] = Field(
        None,
        description="List of tenant IDs. If None, runs for all active tenants.",
    )
    stage_only: bool = Field(
        default=False,
        description="If True, only run Extract + Transform + Load to staging.",
    )
    skip_sources: Optional[list[str]] = Field(
        None,
        description="List of source types to skip (e.g. ['sales', 'inventory']).",
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "tenant_ids": ["STORE_HN", "STORE_HCM"],
                    "stage_only": False,
                    "skip_sources": None,
                }
            ]
        }
    }


class ETLTriggerResponse(BaseModel):
    """Response model for ETL trigger."""
    status: str = Field(..., description="Overall ETL status")
    message: str = Field(..., description="Status message")
    job_id: Optional[str] = Field(None, description="Background job ID if queued")
    results: Optional[dict[str, Any]] = Field(
        None, description="Full ETL results if synchronous"
    )

    model_config = {"from_attributes": True}


class ETLStatusResponse(BaseModel):
    """Response model for ETL status check."""
    status: str = Field(..., description="ETL status")
    scheduler_running: bool = Field(..., description="Is scheduler running")
    next_run: Optional[str] = Field(None, description="Next scheduled run time")
    recent_runs: list[dict[str, Any]] = Field(
        default_factory=list, description="Recent ETL run history"
    )

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Health check models
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    """Response model for health check."""
    status: str = Field(..., description="Overall health status")
    version: str = Field(..., description="API version")
    environment: str = Field(..., description="Environment name")
    timestamp: datetime = Field(
        default_factory=datetime.utcnow, description="Health check timestamp"
    )
    database: str = Field(..., description="Database connectivity status")
    checks: dict[str, Any] = Field(
        default_factory=dict, description="Detailed health checks"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "status": "healthy",
                    "version": "1.0.0",
                    "environment": "development",
                    "timestamp": "2024-03-15T10:30:00Z",
                    "database": "connected",
                    "checks": {},
                }
            ]
        }
    }


# ---------------------------------------------------------------------------
# Error models
# ---------------------------------------------------------------------------

class ErrorResponse(BaseModel):
    """Standard error response model."""
    error: str = Field(..., description="Error type/code")
    message: str = Field(..., description="Human-readable error message")
    detail: Optional[str] = Field(None, description="Detailed error information")
    timestamp: datetime = Field(
        default_factory=datetime.utcnow, description="Error timestamp"
    )

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "error": "AUTHENTICATION_FAILED",
                    "message": "Invalid username or password",
                    "detail": None,
                    "timestamp": "2024-03-15T10:30:00Z",
                }
            ]
        }
    }
