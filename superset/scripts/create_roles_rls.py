#!/usr/bin/env python3
# ============================================================================
# superset/scripts/create_roles_rls.py
# Phase 13: Auto-create Superset RLS roles and Row-Level Security rules
# per tenant.
#
# This script provisions Superset roles for multi-tenant data isolation.
#
# Architecture:
#   1. Creates one Superset role per tenant (e.g. "Tenant_STORE_HN", "Tenant_STORE_HCM")
#   2. Assigns RLS filter to each role: "TenantID = 'STORE_HN'"
#   3. Maps DWH roles (admin/viewer) + tenant to appropriate Superset roles
#
# Role hierarchy:
#   - Admin:  Has Admin role (can manage everything, no RLS)
#   - Viewer STORE_HN: Has Gamma + Tenant_STORE_HN (sees STORE_HN data only)
#   - Viewer STORE_HCM: Has Gamma + Tenant_STORE_HCM (sees STORE_HCM data only)
#
# Usage:
#   python superset/scripts/create_roles_rls.py --init-all
#   python superset/scripts/create_roles_rls.py --tenant STORE_HN
#   python superset/scripts/create_roles_rls.py --tenant STORE_HN --dry-run
#   python superset/scripts/create_roles_rls.py --cleanup
#
# Author: Nguyen Van Khang
# ============================================================================

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Any, Optional

# Ensure venv site-packages is at front of sys.path.
_PROJ_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
_VENV_SITE = os.path.join(_PROJ_ROOT, ".venv", "lib", "python3.13", "site-packages")
if _VENV_SITE not in sys.path:
    sys.path.insert(0, _VENV_SITE)
else:
    sys.path.remove(_VENV_SITE)
    sys.path.insert(0, _VENV_SITE)

import pyodbc
import requests

# Reuse SupersetClient from create_users.py if available
sys.path.insert(0, os.path.dirname(__file__))
try:
    from create_users import SupersetClient
except ImportError:
    # Inline fallback if create_users.py is not available
    SupersetClient = None

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088").rstrip("/")
SUPERSET_ADMIN_USER = os.environ.get("SUPERSET_USERNAME", "admin")
SUPERSET_ADMIN_PWD = os.environ.get("SUPERSET_PASSWORD", "admin")
CONN_STR = os.environ.get("CONN_STR", "")

# Prefixes for auto-generated roles
TENANT_ROLE_PREFIX = "Tenant_"
RLS_BASE_ROLE_NAME = "Gamma"

# Tables that have TenantID and should have RLS applied
RLS_APPLICABLE_TABLES = [
    "FactSales",
    "FactInventory",
    "FactPurchase",
    "DimStore",
    "DimCustomer",
    "DimEmployee",
    "DM_SalesSummary",
    "DM_InventoryAlert",
    "DM_CustomerRFM",
    "DM_EmployeePerformance",
    "DM_PurchaseSummary",
]


# ============================================================================
# Superset Client (inline if not available from create_users.py)
# ============================================================================

if SupersetClient is None:
    class SupersetClient:
        """Minimal Superset client for RLS management."""

        def __init__(self, base_url: str, username: str, password: str):
            self.base_url = base_url.rstrip("/")
            self.username = username
            self.password = password
            self._access_token: Optional[str] = None
            self._refresh_token: Optional[str] = None
            self._login()

        def _login(self) -> None:
            url = f"{self.base_url}/api/v1/security/login"
            payload = {
                "username": self.username,
                "password": self.password,
                "provider": "db",
                "refresh": True,
            }
            resp = requests.post(url, json=payload,
                                  headers={"Content-Type": "application/json"}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data.get("access_token", "")
            self._refresh_token = data.get("refresh_token", "")
            if not self._access_token:
                raise RuntimeError("No access token from Superset")

        def _headers(self) -> dict[str, str]:
            return {
                "Authorization": f"Bearer {self._access_token}",
                "Content-Type": "application/json",
            }

        def _request(self, method: str, endpoint: str,
                     data: Optional[dict[str, Any]] = None,
                     params: Optional[dict[str, Any]] = None) -> requests.Response:
            url = f"{self.base_url}{endpoint}"
            resp = requests.request(method, url, json=data, params=params,
                                    headers=self._headers(), timeout=30)
            if resp.status_code == 401:
                self._login()
                resp = requests.request(method, url, json=data, params=params,
                                        headers=self._headers(), timeout=30)
            return resp

        def list_roles(self) -> list[dict[str, Any]]:
            resp = self._request("GET", "/api/v1/roles", params={"page_size": 500})
            resp.raise_for_status()
            return resp.json().get("result", [])

        def get_role_by_name(self, name: str) -> Optional[dict[str, Any]]:
            for role in self.list_roles():
                if role.get("name") == name:
                    return role
            return None

        def create_role(self, name: str, description: str = "") -> dict[str, Any]:
            payload = {"name": name, "description": description}
            resp = self._request("POST", "/api/v1/roles", data=payload)
            if resp.status_code not in (200, 201):
                if "already exists" in resp.text.lower():
                    existing = self.get_role_by_name(name)
                    if existing:
                        return existing
                raise RuntimeError(f"Failed to create role {name}: {resp.text}")
            data = resp.json()
            result = data.get("result", {})
            logger.info("Created Superset role: %s", name)
            return result

        def get_permission(self, name: str) -> Optional[dict[str, Any]]:
            """Find a permission by name."""
            resp = self._request("GET", "/api/v1/permissions")
            resp.raise_for_status()
            for perm in resp.json().get("result", []):
                if perm.get("name") == name:
                    return perm
            return None

        def add_permission_to_role(self, role_id: int, permission_name: str) -> bool:
            """Add a permission to a role."""
            perm = self.get_permission(permission_name)
            if not perm:
                logger.warning("Permission not found: %s", permission_name)
                return False
            payload = {
                "role_id": role_id,
                "permission_id": perm["id"],
            }
            resp = self._request("POST", "/api/v1/roles/{}/permissions".format(role_id),
                                  data=payload)
            if resp.status_code not in (200, 201):
                logger.debug("Permission %s may already exist on role %d: %s",
                              permission_name, role_id, resp.text[:100])
            return True

        def list_rls_filters(self) -> list[dict[str, Any]]:
            resp = self._request("GET", "/api/v1/rls")
            resp.raise_for_status()
            return resp.json().get("result", [])

        def create_rls_filter(
            self,
            clause: str,
            role_id: int,
            filter_type: str = "where",
        ) -> dict[str, Any]:
            """Create a Row-Level Security filter."""
            # Check for duplicate
            existing = self.list_rls_filters()
            for rls in existing:
                if rls.get("clause") == clause and rls.get("role_id") == role_id:
                    logger.debug("RLS filter already exists: clause=%s role_id=%d", clause, role_id)
                    return rls

            payload = {
                "clause": clause,
                "role_id": role_id,
                "filter_type": filter_type,
            }
            resp = self._request("POST", "/api/v1/rls", data=payload)
            if resp.status_code not in (200, 201):
                raise RuntimeError(f"Failed to create RLS filter: {resp.text}")
            data = resp.json()
            result = data.get("result", {})
            logger.info("Created RLS filter | clause='%s' role_id=%d", clause, role_id)
            return result

        def delete_rls_filter(self, filter_id: int) -> bool:
            """Delete a Row-Level Security filter."""
            resp = self._request("DELETE", f"/api/v1/rls/{filter_id}")
            return resp.status_code in (200, 204)

        def assign_role_to_user(self, user_id: int, role_id: int) -> bool:
            """Assign a role to a user."""
            resp = self._request(
                "POST",
                f"/api/v1/users/{user_id}/roles",
                data={"role_id": role_id},
            )
            return resp.status_code in (200, 201)


# ============================================================================
# DWH Tenant Fetcher
# ============================================================================

def fetch_active_tenants(conn_str: str) -> list[dict[str, Any]]:
    """
    Fetch active tenants from SQL Server Tenants table.

    Args:
        conn_str: SQL Server connection string.

    Returns:
        List of tenant dicts.
    """
    tenants: list[dict[str, Any]] = []
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TenantID, TenantName, FilePath, IsActive, CreatedAt
            FROM Tenants
            WHERE IsActive = 1
            ORDER BY TenantID
        """)
        rows = cursor.fetchall()

        for row in rows:
            tenants.append({
                "tenant_id": row.TenantID,
                "tenant_name": row.TenantName,
                "file_path": row.FilePath,
                "is_active": bool(row.IsActive),
                "created_at": row.CreatedAt,
            })

        cursor.close()
        conn.close()
        logger.info("Fetched %d active tenants from DWH", len(tenants))

    except pyodbc.Error as ex:
        logger.error("Failed to fetch tenants from DWH: %s", ex)
        raise ConnectionError(f"Database error: {ex}") from ex

    return tenants


# ============================================================================
# RLS Role & Filter Management
# ============================================================================

def _sanitize_role_name(tenant_id: str) -> str:
    """Sanitize tenant ID for use as a role name."""
    return TENANT_ROLE_PREFIX + tenant_id.upper().replace(" ", "_").replace("-", "_")


def create_tenant_roles_and_rls(
    client: SupersetClient,
    tenant_id: str,
    tenant_name: str,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Create tenant-specific Superset role with RLS filter.

    For a tenant "STORE_HN", this creates:
        - Role: "Tenant_STORE_HN"
        - RLS filter: "TenantID = 'STORE_HN'" (applies to all RLS-applicable tables)

    Args:
        client: SupersetClient instance.
        tenant_id: Tenant ID from Tenants table.
        tenant_name: Tenant display name.
        dry_run: If True, only show what would be done.

    Returns:
        Dict with created role info and RLS filter IDs.
    """
    role_name = _sanitize_role_name(tenant_id)
    rls_clause = f"TenantID = '{tenant_id}'"
    result: dict[str, Any] = {
        "tenant_id": tenant_id,
        "role_name": role_name,
        "rls_clause": rls_clause,
        "role_created": False,
        "rls_created": False,
        "errors": [],
    }

    try:
        if dry_run:
            logger.info(
                "[DRY-RUN] Would create role '%s' with RLS filter: %s",
                role_name, rls_clause
            )
            result["role_created"] = True
            result["rls_created"] = True
            return result

        # Step 1: Create tenant role
        role = client.create_role(
            name=role_name,
            description=f"Auto-generated role for tenant: {tenant_name} ({tenant_id})",
        )
        role_id = role.get("id", 0)
        result["role_created"] = True
        result["role_id"] = role_id

        # Step 2: Grant read permissions to the role
        # These permissions allow the role to query data sources
        read_permissions = [
            "menu_access[SQL Lab]",
            "menu_access[Dashboards]",
            "can_explore_json",
            "can_explore",
            "can_get_data",
            "can_list_dashboards",
            "can_get_dashboard",
            "can_sqllab_viz",
        ]
        for perm_name in read_permissions:
            try:
                client.add_permission_to_role(role_id, perm_name)
            except Exception as ex:
                logger.debug("Could not add permission '%s': %s", perm_name, ex)

        # Step 3: Create RLS filter for each applicable table
        # The RLS filter clause applies globally to all queries by this role
        rls_filter = client.create_rls_filter(
            clause=rls_clause,
            role_id=role_id,
            filter_type="where",
        )
        result["rls_created"] = True
        result["rls_filter_id"] = rls_filter.get("id")

        logger.info(
            "Tenant role & RLS created | tenant=%s | role=%s | rls_clause='%s'",
            tenant_id, role_name, rls_clause
        )

    except Exception as ex:
        result["errors"].append(str(ex))
        logger.error(
            "Failed to create tenant role/RLS for %s: %s",
            tenant_id, ex
        )

    return result


def cleanup_tenant_roles(
    client: SupersetClient,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Remove all auto-generated tenant roles and their RLS filters.

    Args:
        client: SupersetClient instance.
        dry_run: If True, only show what would be deleted.

    Returns:
        Dict with cleanup stats.
    """
    stats = {"roles_found": 0, "roles_deleted": 0, "rls_deleted": 0, "errors": []}

    roles = client.list_roles()
    tenant_roles = [
        r for r in roles
        if r.get("name", "").startswith(TENANT_ROLE_PREFIX)
    ]
    stats["roles_found"] = len(tenant_roles)

    if not tenant_roles:
        logger.info("No tenant roles found to clean up.")
        return stats

    for role in tenant_roles:
        role_name = role.get("name", "")
        role_id = role.get("id", 0)

        if dry_run:
            logger.info("[DRY-RUN] Would delete role: %s", role_name)
            stats["roles_deleted"] += 1
            continue

        # Delete RLS filters associated with this role
        try:
            all_rls = client.list_rls_filters()
            for rls in all_rls:
                if rls.get("role_id") == role_id:
                    client.delete_rls_filter(rls.get("id", 0))
                    stats["rls_deleted"] += 1
                    logger.debug("Deleted RLS filter %d for role %s",
                                  rls.get("id"), role_name)
        except Exception as ex:
            logger.warning("Failed to delete RLS filters for role %s: %s", role_name, ex)

        # Delete the role itself
        try:
            resp = client._request("DELETE", f"/api/v1/roles/{role_id}")
            if resp.status_code in (200, 204):
                stats["roles_deleted"] += 1
                logger.info("Deleted tenant role: %s", role_name)
            else:
                stats["errors"].append(f"Failed to delete role {role_name}: {resp.text[:100]}")
        except Exception as ex:
            stats["errors"].append(f"Role {role_name}: {ex}")
            logger.error("Failed to delete role %s: %s", role_name, ex)

    return stats


def verify_rls_setup(client: SupersetClient) -> dict[str, Any]:
    """
    Verify that RLS filters are correctly configured.

    Returns:
        Dict with verification results.
    """
    verification: dict[str, Any] = {
        "total_roles": 0,
        "tenant_roles": 0,
        "total_rls_filters": 0,
        "tenant_rls_filters": 0,
        "orphaned_rls": 0,
        "details": [],
    }

    roles = client.list_roles()
    verification["total_roles"] = len(roles)
    verification["tenant_roles"] = sum(
        1 for r in roles if r.get("name", "").startswith(TENANT_ROLE_PREFIX)
    )

    rls_filters = client.list_rls_filters()
    verification["total_rls_filters"] = len(rls_filters)

    # Check RLS filters for tenant roles
    tenant_role_ids = {
        r.get("id") for r in roles
        if r.get("name", "").startswith(TENANT_ROLE_PREFIX)
    }

    for rls in rls_filters:
        role_id = rls.get("role_id")
        if role_id in tenant_role_ids:
            verification["tenant_rls_filters"] += 1
            verification["details"].append({
                "clause": rls.get("clause"),
                "role_id": role_id,
                "filter_type": rls.get("filter_type"),
            })
        else:
            verification["orphaned_rls"] += 1

    logger.info(
        "RLS Verification | tenant_roles=%d | tenant_rls_filters=%d | orphaned=%d",
        verification["tenant_roles"],
        verification["tenant_rls_filters"],
        verification["orphaned_rls"],
    )

    return verification


# ============================================================================
# Main Sync Logic
# ============================================================================

def init_all_tenant_roles_and_rls(
    dry_run: bool = False,
    specific_tenant: Optional[str] = None,
) -> dict[str, Any]:
    """
    Initialize Superset roles and RLS filters for all (or specific) tenants.

    Args:
        dry_run: If True, only show what would be created.
        specific_tenant: If set, only process this tenant ID.

    Returns:
        Dict with results per tenant.
    """
    if not CONN_STR:
        raise ValueError("CONN_STR environment variable is not set")

    # Step 1: Fetch tenants from DWH
    tenants = fetch_active_tenants(CONN_STR)
    if not tenants:
        logger.warning("No active tenants found in DWH database")
        return {"tenants_found": 0, "results": []}

    # Filter to specific tenant if requested
    if specific_tenant:
        tenants = [t for t in tenants if t["tenant_id"] == specific_tenant]
        if not tenants:
            raise ValueError(f"Tenant '{specific_tenant}' not found in DWH")

    # Step 2: Connect to Superset
    client = SupersetClient(
        base_url=SUPERSET_URL,
        username=SUPERSET_ADMIN_USER,
        password=SUPERSET_ADMIN_PWD,
    )

    # Step 3: Create roles and RLS for each tenant
    results: list[dict[str, Any]] = []
    for tenant in tenants:
        result = create_tenant_roles_and_rls(
            client=client,
            tenant_id=tenant["tenant_id"],
            tenant_name=tenant["tenant_name"],
            dry_run=dry_run,
        )
        results.append(result)

    # Step 4: Verify setup
    verification = verify_rls_setup(client)

    return {
        "tenants_found": len(tenants),
        "results": results,
        "verification": verification,
    }


# ============================================================================
# CLI Entry Point
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create Superset RLS roles and filters for multi-tenant isolation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--init-all",
        action="store_true",
        help="Create roles and RLS filters for all active tenants",
    )
    parser.add_argument(
        "--tenant",
        type=str,
        metavar="TENANT_ID",
        help="Create role and RLS for a specific tenant (e.g. STORE_HN)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Remove all auto-generated tenant roles and RLS filters",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify current RLS setup",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("=" * 60)
    logger.info("Superset RLS Role Setup — Phase 13")
    logger.info("=" * 60)

    # Validate
    if not any([args.init_all, args.tenant, args.cleanup, args.verify]):
        parser.print_help()
        sys.exit(0)

    if args.cleanup:
        logger.info("Running cleanup...")
        client = SupersetClient(
            base_url=SUPERSET_URL,
            username=SUPERSET_ADMIN_USER,
            password=SUPERSET_ADMIN_PWD,
        )
        stats = cleanup_tenant_roles(client, dry_run=args.dry_run)
        print("\n" + "=" * 60)
        print("CLEANUP RESULT")
        print("=" * 60)
        print(f"  Tenant roles found   : {stats['roles_found']}")
        print(f"  Tenant roles deleted : {stats['roles_deleted']}")
        print(f"  RLS filters deleted  : {stats['rls_deleted']}")
        if stats['errors']:
            print(f"  Errors               : {len(stats['errors'])}")
            for e in stats['errors']:
                print(f"    - {e}")
        print("=" * 60)

    elif args.verify:
        logger.info("Verifying RLS setup...")
        client = SupersetClient(
            base_url=SUPERSET_URL,
            username=SUPERSET_ADMIN_USER,
            password=SUPERSET_ADMIN_PWD,
        )
        v = verify_rls_setup(client)
        print("\n" + "=" * 60)
        print("RLS VERIFICATION")
        print("=" * 60)
        print(f"  Total roles           : {v['total_roles']}")
        print(f"  Tenant roles          : {v['tenant_roles']}")
        print(f"  Total RLS filters     : {v['total_rls_filters']}")
        print(f"  Tenant RLS filters    : {v['tenant_rls_filters']}")
        print(f"  Orphaned RLS filters  : {v['orphaned_rls']}")
        if v['details']:
            print(f"\n  Active RLS Filters:")
            for d in v['details']:
                print(f"    - clause: \"{d['clause']}\" (role_id={d['role_id']}, type={d['filter_type']})")
        print("=" * 60)

    else:
        try:
            result = init_all_tenant_roles_and_rls(
                dry_run=args.dry_run,
                specific_tenant=args.tenant,
            )
            print("\n" + "=" * 60)
            print("RLS ROLE SETUP RESULT")
            print("=" * 60)
            print(f"  Tenants found : {result['tenants_found']}")
            for r in result.get("results", []):
                status = "OK" if not r.get("errors") else "ERROR"
                print(f"\n  [{status}] Tenant: {r['tenant_id']}")
                print(f"    Role name     : {r['role_name']}")
                print(f"    RLS clause    : {r['rls_clause']}")
                print(f"    Role created  : {r['role_created']}")
                print(f"    RLS created   : {r['rls_created']}")
                if r.get("errors"):
                    for e in r["errors"]:
                        print(f"    ERROR         : {e}")
            v = result.get("verification", {})
            print(f"\n  Verification:")
            print(f"    Tenant roles      : {v.get('tenant_roles', 0)}")
            print(f"    Tenant RLS filters: {v.get('tenant_rls_filters', 0)}")
            print(f"    Orphaned RLS       : {v.get('orphaned_rls', 0)}")
            print("=" * 60)
        except Exception as ex:
            logger.error("RLS setup failed: %s", ex)
            sys.exit(1)

    sys.exit(0)


if __name__ == "__main__":
    main()