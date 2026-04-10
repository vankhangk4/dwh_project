"""
api/routes/tenants.py
Phase 12: Tenant management routes for Auth Gateway.

Provides:
    - GET  /tenants            : List all tenants (admin only).
    - GET  /tenants/me         : Get current user's tenant info.
    - GET  /tenants/{tenant_id}: Get specific tenant details (admin only).

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.database import get_db
from api.middleware import (
    AdminUser,
    AuthenticatedUser,
    TenantScoped,
    inject_tenant_context,
)
from api.models import TenantInfo, TenantListResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tenants", tags=["Tenants"])


@router.get(
    "",
    response_model=TenantListResponse,
    summary="List all tenants",
    description="Get a list of all tenants in the system. Admin only.",
    responses={
        200: {"description": "List of tenants"},
        401: {"description": "Authentication required"},
        403: {"description": "Admin role required"},
    },
)
async def list_tenants(
    db: Annotated[Connection, Depends(get_db)],
    current_user: AdminUser,
) -> TenantListResponse:
    """
    List all tenants.

    **Requires:** Admin role.

    Returns all tenants regardless of active status, with details including
    tenant ID, name, active status, and creation timestamp.
    """
    logger.info(
        "list_tenants | user_id=%d",
        current_user.user_id
    )

    try:
        query = text("""
            SELECT
                TenantID,
                TenantName,
                IsActive,
                CreatedAt
            FROM Tenants
            ORDER BY TenantName ASC
        """)

        result = db.execute(query)
        db.commit()
        rows = result.fetchall()

        tenants = []
        for row in rows:
            tenants.append(TenantInfo(
                tenant_id=row[0],
                tenant_name=row[1],
                is_active=bool(row[2]),
                created_at=row[3],
            ))

        logger.info(
            "list_tenants | total=%d | user_id=%d",
            len(tenants), current_user.user_id
        )

        return TenantListResponse(
            tenants=tenants,
            total=len(tenants),
        )

    except Exception as ex:
        logger.error("Failed to list tenants: %s", ex, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve tenant list: {ex}",
        )


@router.get(
    "/me",
    response_model=TenantInfo,
    summary="Get current user's tenant",
    description="Get the tenant information for the currently authenticated user.",
    responses={
        200: {"description": "Current user's tenant info"},
        401: {"description": "Authentication required"},
        404: {"description": "No tenant associated with user"},
    },
)
async def get_my_tenant(
    db: Annotated[Connection, Depends(get_db)],
    current_user: TenantScoped,
) -> TenantInfo:
    """
    Get the current user's tenant information.

    Returns the tenant details for the tenant associated with the
    authenticated user's token. If the user has no tenant (e.g., super-admin),
    returns 404.
    """
    logger.info(
        "get_my_tenant | user_id=%d | tenant_id=%s",
        current_user.user_id, current_user.tenant_id
    )

    if not current_user.tenant_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No tenant associated with this user account.",
        )

    try:
        inject_tenant_context(db, current_user.tenant_id)

        query = text("""
            SELECT
                TenantID,
                TenantName,
                IsActive,
                CreatedAt
            FROM Tenants
            WHERE TenantID = :tenant_id
        """)

        result = db.execute(query, {"tenant_id": current_user.tenant_id})
        db.commit()
        row = result.fetchone()

        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tenant '{current_user.tenant_id}' not found.",
            )

        return TenantInfo(
            tenant_id=row[0],
            tenant_name=row[1],
            is_active=bool(row[2]),
            created_at=row[3],
        )

    except HTTPException:
        raise
    except Exception as ex:
        logger.error("Failed to get tenant for user: %s", ex, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve tenant info: {ex}",
        )


@router.get(
    "/{tenant_id}",
    response_model=TenantInfo,
    summary="Get tenant by ID",
    description="Get details of a specific tenant by its ID. Admin only.",
    responses={
        200: {"description": "Tenant details"},
        401: {"description": "Authentication required"},
        403: {"description": "Admin role required"},
        404: {"description": "Tenant not found"},
    },
)
async def get_tenant_by_id(
    tenant_id: str,
    db: Annotated[Connection, Depends(get_db)],
    current_user: AdminUser,
) -> TenantInfo:
    """
    Get tenant details by tenant ID.

    **Requires:** Admin role.

    Args:
        tenant_id: The tenant identifier.

    Returns full tenant information including active status and creation date.
    """
    logger.info(
        "get_tenant | tenant_id=%s | user_id=%d",
        tenant_id, current_user.user_id
    )

    try:
        query = text("""
            SELECT
                TenantID,
                TenantName,
                IsActive,
                CreatedAt
            FROM Tenants
            WHERE TenantID = :tenant_id
        """)

        result = db.execute(query, {"tenant_id": tenant_id})
        db.commit()
        row = result.fetchone()

        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Tenant '{tenant_id}' not found.",
            )

        return TenantInfo(
            tenant_id=row[0],
            tenant_name=row[1],
            is_active=bool(row[2]),
            created_at=row[3],
        )

    except HTTPException:
        raise
    except Exception as ex:
        logger.error(
            "Failed to get tenant %s: %s",
            tenant_id, ex, exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve tenant info: {ex}",
        )
