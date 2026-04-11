"""
api/routes/superset.py
Phase 13: Superset integration routes for Auth Gateway.

Provides:
    - POST /superset/users/sync        : Sync DWH users to Superset (admin only)
    - GET  /superset/users             : List Superset users
    - GET  /superset/roles            : List Superset roles
    - POST /superset/roles/init       : Initialize tenant RLS roles (admin only)
    - GET  /superset/rls              : List RLS filters
    - POST /superset/rls              : Create RLS filter (admin only)
    - DELETE /superset/rls/{id}       : Delete RLS filter (admin only)
    - GET  /superset/dashboards        : List Superset dashboards
    - POST /superset/dashboards/seed   : Seed dashboard scaffolds (admin only)
    - GET  /superset/health            : Superset health check
    - POST /superset/provision/tenant : Full tenant provisioning (admin only)

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy.engine import Connection

from api.database import get_db
from api.middleware import AdminUser, AuthenticatedUser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/superset", tags=["Superset"])

# Ensure superset module is on path
_PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if _PROJ_ROOT not in sys.path:
    sys.path.insert(0, _PROJ_ROOT)

# ============================================================================
# Pydantic Models
# ============================================================================

class SyncUsersRequest(BaseModel):
    """Request to sync DWH users to Superset."""
    tenant_id: Optional[str] = Field(
        None,
        description="Sync only users for a specific tenant. If None, sync all."
    )
    dry_run: bool = Field(
        False,
        description="If True, show what would be done without making changes."
    )
    model_config = {
        "json_schema_extra": {
            "examples": [
                {"tenant_id": None, "dry_run": False},
                {"tenant_id": "STORE_HN", "dry_run": True},
            ]
        }
    }


class SyncUsersResponse(BaseModel):
    """Response from user sync operation."""
    status: str = Field(..., description="Overall sync status")
    message: str = Field(..., description="Status message")
    total_users: int = Field(0, description="Total users scanned")
    created: int = Field(0, description="Number of users created in Superset")
    updated: int = Field(0, description="Number of users updated")
    skipped: int = Field(0, description="Number skipped (dry-run or no action)")
    failed: int = Field(0, description="Number of failures")
    errors: list[str] = Field(default_factory=list, description="Error details")


class InitRolesRequest(BaseModel):
    """Request to initialize tenant RLS roles."""
    tenant_id: Optional[str] = Field(
        None,
        description="Specific tenant ID. If None, initialize for all tenants."
    )
    dry_run: bool = Field(False, description="Show what would be created.")


class InitRolesResponse(BaseModel):
    """Response from RLS role initialization."""
    status: str = Field(..., description="Overall status")
    tenants_found: int = Field(0, description="Number of tenants processed")
    results: list[dict[str, Any]] = Field(
        default_factory=list, description="Result per tenant"
    )
    verification: dict[str, Any] = Field(
        default_factory=dict, description="RLS verification result"
    )


class CreateRLSRequest(BaseModel):
    """Request to create an RLS filter."""
    clause: str = Field(..., description="SQL WHERE clause (e.g. \"TenantID = 'STORE_HN'\")")
    role_id: int = Field(..., description="Superset role ID to apply filter to")
    filter_type: str = Field(
        default="where",
        description="Filter type: 'where' or 'having'"
    )
    model_config = {
        "json_schema_extra": {
            "examples": [
                {"clause": "TenantID = 'STORE_HN'", "role_id": 5, "filter_type": "where"}
            ]
        }
    }


class SeedDashboardsRequest(BaseModel):
    """Request to seed dashboard scaffolds."""
    dashboard_ids: Optional[list[int]] = Field(
        None,
        description="Specific dashboard IDs to create. If None, create all 5."
    )
    dry_run: bool = Field(False, description="Show what would be created.")


class SeedDashboardsResponse(BaseModel):
    """Response from dashboard seeding."""
    status: str = Field(..., description="Overall status")
    total_definitions: int = Field(0, description="Number of dashboard definitions")
    created_or_exists: int = Field(0, description="Number created or already exists")
    dry_run_skipped: int = Field(0, description="Number skipped (dry-run)")
    results: list[dict[str, Any]] = Field(default_factory=list)


class SupersetHealthResponse(BaseModel):
    """Response from Superset health check."""
    status: str = Field(..., description="Superset health status")
    version: str = Field(..., description="Superset version")
    database_ok: bool = Field(..., description="Database connectivity")
    celery_active: bool = Field(..., description="Celery worker status")
    url: str = Field(..., description="Superset URL")


class ProvisionTenantRequest(BaseModel):
    """Request to provision a tenant in Superset."""
    tenant_id: str = Field(..., description="Tenant ID to provision")
    tenant_name: str = Field(..., description="Tenant display name")


class ProvisionTenantResponse(BaseModel):
    """Response from tenant provisioning."""
    status: str = Field(..., description="Provisioning status")
    tenant_id: str = Field(..., description="Tenant ID")
    role_name: str = Field(..., description="Created role name")
    role_id: Optional[int] = Field(None, description="Role ID in Superset")
    rls_filter_id: Optional[int] = Field(None, description="RLS filter ID")
    errors: list[str] = Field(default_factory=list)


# ============================================================================
# Health Check
# ============================================================================

@router.get(
    "/health",
    response_model=SupersetHealthResponse,
    summary="Superset health check",
    description="Check if Superset is running and accessible.",
    responses={200: {"description": "Health check result"}},
)
async def superset_health(
    current_user: AuthenticatedUser,
) -> SupersetHealthResponse:
    """
    Check Superset service health.

    Args:
        current_user: Authenticated user.

    Returns:
        SupersetHealthResponse with service status.
    """
    from api.config import get_config

    logger.debug("Superset health check requested by user_id=%d", current_user.user_id)
    config = get_config()

    try:
        from superset.superset_client import SupersetClient

        client = SupersetClient(
            base_url=config.superset.url,
            username=config.superset.username,
            password=config.superset.password,
            timeout=10,
        )
        health = client.health_check()
        return SupersetHealthResponse(
            status=health.status,
            version=health.version,
            database_ok=health.database_ok,
            celery_active=health.celery_active,
            url=config.superset.url,
        )

    except Exception as ex:
        logger.error("Superset health check failed: %s", ex)
        return SupersetHealthResponse(
            status="DOWN",
            version="",
            database_ok=False,
            celery_active=False,
            url=config.superset.url,
        )


# ============================================================================
# User Management
# ============================================================================

@router.post(
    "/users/sync",
    response_model=SyncUsersResponse,
    summary="Sync DWH users to Superset",
    description="""
    Synchronize users from the DWH AppUsers table to Superset.

    **Requires:** Admin role.

    This endpoint:
    1. Reads active users from the AppUsers table in SQL Server.
    2. For each user, creates or updates their Superset account.
    3. Maps DWH roles to Superset roles (admin→Admin, viewer→Gamma, editor→Alpha).
    4. Stores DWH metadata (tenant_id, user_id) in Superset's extra field.

    Use ?dry_run=true to preview changes without applying them.
    """,
    responses={
        200: {"description": "Sync completed"},
        401: {"description": "Authentication required"},
        403: {"description": "Admin role required"},
        503: {"description": "Superset unavailable"},
    },
)
async def sync_users_to_superset(
    request: SyncUsersRequest,
    current_user: AdminUser,
) -> SyncUsersResponse:
    """
    Sync DWH AppUsers to Superset.

    **Requires:** Admin role.

    Args:
        request: SyncUsersRequest with tenant_id and dry_run.
        current_user: Authenticated admin user.

    Returns:
        SyncUsersResponse with sync statistics.
    """
    logger.info(
        "sync_users_to_superset | user_id=%d | tenant_id=%s | dry_run=%s",
        current_user.user_id,
        request.tenant_id or "ALL",
        request.dry_run,
    )

    try:
        # Import sync function from superset scripts
        sys.path.insert(0, os.path.join(_PROJ_ROOT, "superset", "scripts"))
        from create_users import sync_users_to_superset as _sync_fn

        result = _sync_fn(
            dry_run=request.dry_run,
            tenant_id=request.tenant_id,
        )

        return SyncUsersResponse(
            status="SUCCESS" if result.failed == 0 else "PARTIAL",
            message=f"Sync completed — {result.created} created, {result.updated} updated, {result.skipped} skipped, {result.failed} failed",
            total_users=result.total_users,
            created=result.created,
            updated=result.updated,
            skipped=result.skipped,
            failed=result.failed,
            errors=result.errors,
        )

    except Exception as ex:
        logger.error("User sync failed: %s", ex, exc_info=True)
        return SyncUsersResponse(
            status="FAILED",
            message=f"User sync failed: {ex}",
            failed=1,
            errors=[str(ex)],
        )


@router.get(
    "/users",
    summary="List Superset users",
    description="Get all users registered in Superset.",
    responses={200: {"description": "List of Superset users"}},
)
async def list_superset_users(
    current_user: AuthenticatedUser,
    page: int = Query(default=0, ge=0, description="Page number"),
    page_size: int = Query(default=100, ge=1, le=500, description="Page size"),
) -> dict[str, Any]:
    """
    List all users in Superset.

    Args:
        current_user: Authenticated user.
        page: Page number (0-indexed).
        page_size: Results per page.

    Returns:
        Dict with count and list of user objects.
    """
    from api.config import get_config

    logger.debug("list_superset_users | user_id=%d", current_user.user_id)
    config = get_config()

    try:
        from superset.superset_client import SupersetClient

        client = SupersetClient(
            base_url=config.superset.url,
            username=config.superset.username,
            password=config.superset.password,
        )
        users = client.list_users(page=page, page_size=page_size)

        return {
            "count": len(users),
            "total": len(users),  # Simplified; real count would need pagination
            "users": [
                {
                    "id": u.id,
                    "username": u.username,
                    "email": u.email,
                    "first_name": u.first_name,
                    "last_name": u.last_name,
                    "is_active": u.is_active,
                    "roles": u.roles,
                    "extra": u.extra,
                }
                for u in users
            ],
        }

    except Exception as ex:
        logger.error("Failed to list Superset users: %s", ex)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to connect to Superset: {ex}",
        )


# ============================================================================
# Role & RLS Management
# ============================================================================

@router.get(
    "/roles",
    summary="List Superset roles",
    description="Get all roles available in Superset.",
    responses={200: {"description": "List of Superset roles"}},
)
async def list_superset_roles(
    current_user: AuthenticatedUser,
) -> dict[str, Any]:
    """List all Superset roles."""
    from api.config import get_config

    config = get_config()
    try:
        from superset.superset_client import SupersetClient

        client = SupersetClient(
            base_url=config.superset.url,
            username=config.superset.username,
            password=config.superset.password,
        )
        roles = client.list_roles()

        return {
            "count": len(roles),
            "roles": [
                {
                    "id": r.id,
                    "name": r.name,
                    "description": r.description,
                    "permissions_count": len(r.permissions),
                }
                for r in roles
            ],
        }

    except Exception as ex:
        logger.error("Failed to list Superset roles: %s", ex)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to connect to Superset: {ex}",
        )


@router.post(
    "/roles/init",
    response_model=InitRolesResponse,
    summary="Initialize tenant RLS roles",
    description="""
    Initialize Superset RLS roles for all (or specific) tenants.

    **Requires:** Admin role.

    For each active tenant in the DWH database, this creates:
        - A Superset role named "Tenant_{tenant_id}" (e.g. "Tenant_STORE_HN")
        - An RLS filter: "TenantID = '{tenant_id}'"
        - Basic read permissions for the role
    """,
    responses={
        200: {"description": "RLS roles initialized"},
        401: {"description": "Authentication required"},
        403: {"description": "Admin role required"},
        503: {"description": "Superset unavailable"},
    },
)
async def init_tenant_roles(
    request: InitRolesRequest,
    current_user: AdminUser,
) -> InitRolesResponse:
    """
    Initialize Superset RLS roles for tenants.

    **Requires:** Admin role.

    Args:
        request: InitRolesRequest with tenant_id filter and dry_run flag.
        current_user: Authenticated admin user.

    Returns:
        InitRolesResponse with per-tenant results.
    """
    logger.info(
        "init_tenant_roles | user_id=%d | tenant_id=%s | dry_run=%s",
        current_user.user_id,
        request.tenant_id or "ALL",
        request.dry_run,
    )

    try:
        sys.path.insert(0, os.path.join(_PROJ_ROOT, "superset", "scripts"))
        from create_roles_rls import init_all_tenant_roles_and_rls as _init_fn

        result = _init_fn(
            dry_run=request.dry_run,
            specific_tenant=request.tenant_id,
        )

        all_ok = all(
            not r.get("errors") for r in result.get("results", [])
        )

        return InitRolesResponse(
            status="SUCCESS" if all_ok else "PARTIAL_ERROR",
            tenants_found=result.get("tenants_found", 0),
            results=result.get("results", []),
            verification=result.get("verification", {}),
        )

    except Exception as ex:
        logger.error("Tenant roles init failed: %s", ex, exc_info=True)
        return InitRolesResponse(
            status="FAILED",
            tenants_found=0,
            results=[],
            verification={},
        )


@router.get(
    "/rls",
    summary="List RLS filters",
    description="Get all Row-Level Security filters in Superset.",
    responses={200: {"description": "List of RLS filters"}},
)
async def list_rls_filters(
    current_user: AuthenticatedUser,
) -> dict[str, Any]:
    """List all RLS filters."""
    from api.config import get_config

    config = get_config()
    try:
        from superset.superset_client import SupersetClient

        client = SupersetClient(
            base_url=config.superset.url,
            username=config.superset.username,
            password=config.superset.password,
        )
        filters = client.list_rls_filters()

        return {
            "count": len(filters),
            "filters": [
                {
                    "id": f.id,
                    "clause": f.clause,
                    "role_id": f.role_id,
                    "role_name": f.role_name,
                    "filter_type": f.filter_type,
                }
                for f in filters
            ],
        }

    except Exception as ex:
        logger.error("Failed to list RLS filters: %s", ex)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to connect to Superset: {ex}",
        )


@router.post(
    "/rls",
    response_model=dict[str, Any],
    summary="Create RLS filter",
    description="Create a Row-Level Security filter for a role.",
    responses={
        200: {"description": "RLS filter created"},
        401: {"description": "Authentication required"},
        403: {"description": "Admin role required"},
    },
)
async def create_rls_filter(
    request: CreateRLSRequest,
    current_user: AdminUser,
) -> dict[str, Any]:
    """
    Create an RLS filter.

    **Requires:** Admin role.

    Args:
        request: CreateRLSRequest with clause, role_id, and filter_type.
        current_user: Authenticated admin user.

    Returns:
        Created RLS filter details.
    """
    from api.config import get_config

    logger.info(
        "create_rls_filter | user_id=%d | role_id=%d | clause='%s'",
        current_user.user_id, request.role_id, request.clause
    )
    config = get_config()

    try:
        from superset.superset_client import SupersetClient

        client = SupersetClient(
            base_url=config.superset.url,
            username=config.superset.username,
            password=config.superset.password,
        )
        rls = client.create_rls_filter(
            clause=request.clause,
            role_id=request.role_id,
            filter_type=request.filter_type,
        )

        return {
            "status": "CREATED",
            "id": rls.id,
            "clause": rls.clause,
            "role_id": rls.role_id,
            "filter_type": rls.filter_type,
        }

    except Exception as ex:
        logger.error("Failed to create RLS filter: %s", ex)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create RLS filter: {ex}",
        )


@router.delete(
    "/rls/{filter_id}",
    summary="Delete RLS filter",
    description="Delete a Row-Level Security filter by ID.",
    responses={
        200: {"description": "RLS filter deleted"},
        401: {"description": "Authentication required"},
        403: {"description": "Admin role required"},
    },
)
async def delete_rls_filter(
    filter_id: int,
    current_user: AdminUser,
) -> dict[str, str]:
    """
    Delete an RLS filter.

    **Requires:** Admin role.
    """
    from api.config import get_config

    logger.info(
        "delete_rls_filter | user_id=%d | filter_id=%d",
        current_user.user_id, filter_id
    )
    config = get_config()

    try:
        from superset.superset_client import SupersetClient

        client = SupersetClient(
            base_url=config.superset.url,
            username=config.superset.username,
            password=config.superset.password,
        )
        success = client.delete_rls_filter(filter_id)
        if success:
            return {"status": "DELETED", "filter_id": filter_id}
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"RLS filter {filter_id} not found",
        )

    except HTTPException:
        raise
    except Exception as ex:
        logger.error("Failed to delete RLS filter %d: %s", filter_id, ex)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete RLS filter: {ex}",
        )


# ============================================================================
# Dashboard Management
# ============================================================================

@router.get(
    "/dashboards",
    summary="List Superset dashboards",
    description="Get all dashboards available in Superset.",
    responses={200: {"description": "List of dashboards"}},
)
async def list_dashboards(
    current_user: AuthenticatedUser,
) -> dict[str, Any]:
    """List all Superset dashboards."""
    from api.config import get_config

    config = get_config()
    try:
        from superset.superset_client import SupersetClient

        client = SupersetClient(
            base_url=config.superset.url,
            username=config.superset.username,
            password=config.superset.password,
        )
        dashboards = client.list_dashboards()

        return {
            "count": len(dashboards),
            "dashboards": [
                {
                    "id": d.id,
                    "title": d.dashboard_title,
                    "slug": d.slug,
                    "published": d.published,
                    "description": d.description,
                    "url": d.url,
                }
                for d in dashboards
            ],
        }

    except Exception as ex:
        logger.error("Failed to list dashboards: %s", ex)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Failed to connect to Superset: {ex}",
        )


@router.post(
    "/dashboards/seed",
    response_model=SeedDashboardsResponse,
    summary="Seed dashboard scaffolds",
    description="""
    Create 5 dashboard scaffolds in Superset.

    **Requires:** Admin role.

    Dashboards created:
        1. Sales Overview (id=1)
        2. Inventory Management (id=2)
        3. Customer Analytics (id=3)
        4. Employee Performance (id=4)
        5. Purchase Overview (id=5)

    Each dashboard includes pre-configured charts with:
        - Correct viz_type and datasource
        - Appropriate metrics and groupby
        - Proper grid positioning
    """,
    responses={
        200: {"description": "Dashboards seeded"},
        401: {"description": "Authentication required"},
        403: {"description": "Admin role required"},
        503: {"description": "Superset unavailable"},
    },
)
async def seed_dashboards(
    request: SeedDashboardsRequest,
    current_user: AdminUser,
) -> SeedDashboardsResponse:
    """
    Seed dashboard scaffolds in Superset.

    **Requires:** Admin role.

    Args:
        request: SeedDashboardsRequest with optional dashboard_ids filter.
        current_user: Authenticated admin user.

    Returns:
        SeedDashboardsResponse with seeding results.
    """
    logger.info(
        "seed_dashboards | user_id=%d | dashboard_ids=%s | dry_run=%s",
        current_user.user_id,
        request.dashboard_ids or "ALL",
        request.dry_run,
    )

    try:
        from api.config import get_config
        from superset.superset_client import SupersetClient

        config = get_config()
        client = SupersetClient(
            base_url=config.superset.url,
            username=config.superset.username,
            password=config.superset.password,
        )

        sys.path.insert(0, os.path.join(_PROJ_ROOT, "superset", "scripts"))
        from seed_dashboards import seed_all_dashboards as _seed_fn

        result = _seed_fn(
            client=client,
            dry_run=request.dry_run,
            dashboard_ids=request.dashboard_ids,
        )

        return SeedDashboardsResponse(
            status="SUCCESS",
            total_definitions=result["total_definitions"],
            created_or_exists=result["created_or_exists"],
            dry_run_skipped=result["dry_run_skipped"],
            results=result["results"],
        )

    except Exception as ex:
        logger.error("Dashboard seeding failed: %s", ex, exc_info=True)
        return SeedDashboardsResponse(
            status="FAILED",
            total_definitions=0,
            created_or_exists=0,
            dry_run_skipped=0,
            results=[],
        )


# ============================================================================
# Tenant Provisioning
# ============================================================================

@router.post(
    "/provision/tenant",
    response_model=ProvisionTenantResponse,
    summary="Provision tenant in Superset",
    description="""
    Full tenant provisioning: create RLS role + RLS filter.

    **Requires:** Admin role.

    This is a convenience endpoint that wraps role creation + RLS filter
    into a single atomic operation.
    """,
    responses={
        200: {"description": "Tenant provisioned"},
        401: {"description": "Authentication required"},
        403: {"description": "Admin role required"},
    },
)
async def provision_tenant(
    request: ProvisionTenantRequest,
    current_user: AdminUser,
) -> ProvisionTenantResponse:
    """
    Provision a tenant in Superset.

    **Requires:** Admin role.

    Args:
        request: ProvisionTenantRequest with tenant_id and tenant_name.
        current_user: Authenticated admin user.

    Returns:
        ProvisionTenantResponse with provisioning details.
    """
    from api.config import get_config

    logger.info(
        "provision_tenant | user_id=%d | tenant_id=%s",
        current_user.user_id, request.tenant_id
    )
    config = get_config()

    try:
        from superset.superset_client import SupersetClient

        client = SupersetClient(
            base_url=config.superset.url,
            username=config.superset.username,
            password=config.superset.password,
        )

        result = client.provision_tenant(
            tenant_id=request.tenant_id,
            tenant_name=request.tenant_name,
        )

        return ProvisionTenantResponse(
            status=result["status"],
            tenant_id=result["tenant_id"],
            role_name=result["role_name"],
            role_id=result.get("role_id"),
            rls_filter_id=result.get("rls_filter_id"),
            errors=result.get("errors", []),
        )

    except Exception as ex:
        logger.error("Tenant provisioning failed for %s: %s", request.tenant_id, ex)
        return ProvisionTenantResponse(
            status="ERROR",
            tenant_id=request.tenant_id,
            role_name="",
            errors=[str(ex)],
        )


@router.delete(
    "/provision/tenant/{tenant_id}",
    summary="Deprovision tenant",
    description="Remove tenant's RLS role and filters from Superset.",
    responses={
        200: {"description": "Tenant deprovisioned"},
        401: {"description": "Authentication required"},
        403: {"description": "Admin role required"},
    },
)
async def deprovision_tenant(
    tenant_id: str,
    current_user: AdminUser,
) -> dict[str, Any]:
    """
    Deprovision a tenant from Superset.

    **Requires:** Admin role.

    Removes the tenant's RLS role and all associated RLS filters.
    """
    from api.config import get_config

    logger.info(
        "deprovision_tenant | user_id=%d | tenant_id=%s",
        current_user.user_id, tenant_id
    )
    config = get_config()

    try:
        from superset.superset_client import SupersetClient

        client = SupersetClient(
            base_url=config.superset.url,
            username=config.superset.username,
            password=config.superset.password,
        )

        result = client.deprovision_tenant(tenant_id)
        return result

    except Exception as ex:
        logger.error("Tenant deprovisioning failed for %s: %s", tenant_id, ex)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to deprovision tenant: {ex}",
        )
