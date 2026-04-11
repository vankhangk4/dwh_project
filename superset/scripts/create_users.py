#!/usr/bin/env python3
# ============================================================================
# superset/scripts/create_users.py
# Phase 13: Batch create Superset users from AppUsers table.
#
# This script is called by the Auth Gateway API (api/routes/superset.py)
# to provision Superset accounts for DWH users.
#
# It reads AppUsers from SQL Server, then creates corresponding users
# in Superset with appropriate roles based on their role in the DWH system.
#
# Mapping:
#   DWH Role "admin"  → Superset role "Admin"
#   DWH Role "viewer" → Superset role "Gamma"
#   DWH Role "editor" → Superset role "Alpha"
#
# Each user is also associated with their tenant via a custom property
# (stored in Superset's extra field) for RLS filter assignment.
#
# Usage:
#   # Direct execution (requires .env)
#   python superset/scripts/create_users.py --dry-run
#   python superset/scripts/create_users.py --sync-all
#   python superset/scripts/create_users.py --sync-tenant STORE_HN
#
#   # Via API (recommended)
#   POST /superset/users/sync        (admin only)
#   POST /superset/users/sync?tenant_id=STORE_HN
#
# Author: Nguyen Van Khang
# ============================================================================

from __future__ import annotations

import argparse
import logging
import os
import sys

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

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088").rstrip("/")
SUPERSET_ADMIN_USER = os.environ.get("SUPERSET_USERNAME", "admin")
SUPERSET_ADMIN_PWD = os.environ.get("SUPERSET_PASSWORD", "admin")
CONN_STR = os.environ.get("CONN_STR", "")

# Role mapping from DWH role to Superset role
DWH_ROLE_TO_SUPERSET_ROLE: dict[str, list[str]] = {
    "admin": ["Admin"],
    "viewer": ["Gamma"],
    "editor": ["Alpha"],
}


# ============================================================================
# Dataclasses
# ============================================================================

@dataclass
class DWHUser:
    """Represents a DWH user from AppUsers table."""
    user_id: int
    username: str
    role: str
    tenant_id: Optional[str]
    tenant_name: Optional[str]
    email: Optional[str] = None
    is_active: bool = True

    def get_superset_roles(self) -> list[str]:
        """Map DWH role to Superset roles."""
        return DWH_ROLE_TO_SUPERSET_ROLE.get(self.role, ["Gamma"])


@dataclass
class SyncResult:
    """Result of a sync operation."""
    total_users: int = 0
    created: int = 0
    updated: int = 0
    skipped: int = 0
    failed: int = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_users": self.total_users,
            "created": self.created,
            "updated": self.updated,
            "skipped": self.skipped,
            "failed": self.failed,
            "errors": self.errors,
        }


# ============================================================================
# Superset API Client
# ============================================================================

class SupersetClient:
    """
    Low-level client for Superset REST API v1.

    Handles authentication, token management, and HTTP requests.
    """

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._login()

    def _login(self) -> None:
        """Authenticate with Superset and store access token."""
        url = f"{self.base_url}/api/v1/security/login"
        payload = {
            "username": self.username,
            "password": self.password,
            "provider": "db",
            "refresh": True,
        }

        try:
            resp = requests.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
        except requests.RequestException as ex:
            raise ConnectionError(
                f"Failed to login to Superset at {url}: {ex}"
            ) from ex

        data = resp.json()
        self._access_token = data.get("access_token", "")
        self._refresh_token = data.get("refresh_token", "")

        if not self._access_token:
            raise RuntimeError("No access token returned from Superset login")

        logger.debug("Superset login successful | user=%s", self.username)

    def _refresh_access_token(self) -> None:
        """Refresh the access token using the refresh token."""
        url = f"{self.base_url}/api/v1/security/refresh"
        try:
            resp = requests.post(
                url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self._refresh_token}",
                },
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data.get("access_token", "")
        except requests.RequestException as ex:
            logger.warning("Token refresh failed: %s. Re-authenticating.", ex)
            self._login()

    def _get_headers(self) -> dict[str, str]:
        """Return HTTP headers with Authorization token."""
        if not self._access_token:
            self._login()
        return {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> requests.Response:
        """
        Make an authenticated HTTP request to Superset API.

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE).
            endpoint: API endpoint path (e.g. "/api/v1/users").
            data: JSON payload body.
            params: Query string parameters.

        Returns:
            requests.Response object.

        Raises:
            requests.RequestException on HTTP error.
        """
        url = f"{self.base_url}{endpoint}"
        headers = self._get_headers()

        try:
            resp = requests.request(
                method,
                url,
                json=data,
                params=params,
                headers=headers,
                timeout=30,
            )

            # Handle token expiry
            if resp.status_code == 401:
                logger.debug("Token expired, refreshing...")
                self._refresh_access_token()
                headers = self._get_headers()
                resp = requests.request(
                    method, url, json=data, params=params,
                    headers=headers, timeout=30,
                )

            return resp

        except requests.RequestException as ex:
            logger.error("Superset API request failed | %s %s: %s", method, url, ex)
            raise

    # --------------------------------------------------------------------------
    # User management
    # --------------------------------------------------------------------------

    def list_users(self, page: int = 0, page_size: int = 100) -> list[dict[str, Any]]:
        """
        List all users in Superset.

        Args:
            page: Page number (0-indexed).
            page_size: Number of users per page.

        Returns:
            List of user dicts from Superset.
        """
        resp = self._request(
            "GET",
            "/api/v1/users",
            params={"page": page, "page_size": page_size},
        )
        resp.raise_for_status()
        payload = resp.json()
        return payload.get("result", [])

    def get_user_by_username(self, username: str) -> Optional[dict[str, Any]]:
        """
        Find a Superset user by username.

        Args:
            username: Username to look up.

        Returns:
            User dict if found, None otherwise.
        """
        users = self.list_users()
        for user in users:
            if user.get("username") == username:
                return user
        return None

    def get_user_by_email(self, email: str) -> Optional[dict[str, Any]]:
        """Find a Superset user by email address."""
        resp = self._request(
            "GET",
            "/api/v1/users",
            params={"filters": [{"col": "email", "opr": "eq", "value": email}]},
        )
        resp.raise_for_status()
        payload = resp.json()
        results = payload.get("result", [])
        return results[0] if results else None

    def create_user(
        self,
        username: str,
        email: str,
        first_name: str,
        last_name: str,
        roles: list[str],
        extra: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Create a new Superset user.

        Args:
            username: Unique username.
            email: Email address.
            first_name: First name.
            last_name: Last name.
            roles: List of role names to assign.
            extra: Custom metadata (stored as JSON string in Superset).

        Returns:
            Created user dict.

        Raises:
            RuntimeError if creation fails.
        """
        payload: dict[str, Any] = {
            "username": username,
            "email": email,
            "first_name": first_name,
            "last_name": last_name,
            "roles": roles,
        }
        if extra:
            payload["extra"] = json.dumps(extra)

        resp = self._request("POST", "/api/v1/users", data=payload)
        if resp.status_code not in (200, 201):
            body = resp.text
            # Handle "user already exists" gracefully
            if "already exists" in body.lower() or "unique" in body.lower():
                logger.debug("User %s already exists in Superset", username)
                existing = self.get_user_by_username(username)
                if existing:
                    return existing
            raise RuntimeError(
                f"Failed to create user {username}: HTTP {resp.status_code} — {body}"
            )

        data = resp.json()
        result = data.get("result", {})
        logger.info("User created in Superset | username=%s | roles=%s", username, roles)
        return result

    def update_user(
        self,
        user_id: int,
        roles: Optional[list[str]] = None,
        extra: Optional[dict[str, Any]] = None,
        is_active: Optional[bool] = None,
    ) -> dict[str, Any]:
        """
        Update an existing Superset user.

        Args:
            user_id: Superset user ID.
            roles: New list of roles to assign.
            extra: Custom metadata to update.
            is_active: Account active status.

        Returns:
            Updated user dict.
        """
        payload: dict[str, Any] = {}
        if roles is not None:
            payload["roles"] = roles
        if extra is not None:
            payload["extra"] = json.dumps(extra)
        if is_active is not None:
            payload["is_active"] = is_active

        resp = self._request("PUT", f"/api/v1/users/{user_id}", data=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to update user {user_id}: HTTP {resp.status_code} — {resp.text}"
            )

        data = resp.json()
        return data.get("result", {})

    def get_roles(self) -> list[dict[str, Any]]:
        """List all available roles in Superset."""
        resp = self._request("GET", "/api/v1/roles", params={"page_size": 100})
        resp.raise_for_status()
        data = resp.json()
        return data.get("result", [])

    def get_role_by_name(self, name: str) -> Optional[dict[str, Any]]:
        """Find a Superset role by name."""
        roles = self.get_roles()
        for role in roles:
            if role.get("name") == name:
                return role
        return None

    # --------------------------------------------------------------------------
    # RLS management
    # --------------------------------------------------------------------------

    def list_rls_filters(self) -> list[dict[str, Any]]:
        """List all Row-Level Security filters."""
        resp = self._request("GET", "/api/v1/rls")
        resp.raise_for_status()
        return resp.json().get("result", [])

    def create_rls_filter(
        self,
        clause: str,
        role_id: int,
        filter_type: str = "where",
    ) -> dict[str, Any]:
        """
        Create a Row-Level Security filter.

        Args:
            clause: SQL WHERE clause (e.g. "TenantID = 'STORE_HN'").
            role_id: Superset role ID to apply this filter to.
            filter_type: Filter type ("where" or "having").

        Returns:
            Created RLS filter dict.
        """
        payload = {
            "clause": clause,
            "role_id": role_id,
            "filter_type": filter_type,
        }
        resp = self._request("POST", "/api/v1/rls", data=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to create RLS filter: HTTP {resp.status_code} — {resp.text}"
            )
        data = resp.json()
        return data.get("result", {})

    def delete_rls_filter(self, filter_id: int) -> bool:
        """Delete a Row-Level Security filter."""
        resp = self._request("DELETE", f"/api/v1/rls/{filter_id}")
        return resp.status_code in (200, 204)


# ============================================================================
# DWH User Fetcher
# ============================================================================

def fetch_dwh_users(
    conn_str: str,
    tenant_id: Optional[str] = None,
) -> list[DWHUser]:
    """
    Fetch users from SQL Server AppUsers table.

    Args:
        conn_str: SQL Server connection string.
        tenant_id: Optional tenant ID to filter by. If None, fetches all users.

    Returns:
        List of DWHUser objects.
    """
    import pyodbc

    users: list[DWHUser] = []
    try:
        conn = pyodbc.connect(conn_str, timeout=30)
        cursor = conn.cursor()

        query = """
            SELECT
                u.UserID,
                u.Username,
                u.Role,
                u.TenantID,
                t.TenantName,
                u.IsActive
            FROM AppUsers u
            LEFT JOIN Tenants t ON u.TenantID = t.TenantID
            WHERE u.IsActive = 1
        """
        params: list[Any] = []
        if tenant_id:
            query += " AND u.TenantID = ?"
            params.append(tenant_id)

        query += " ORDER BY u.UserID"

        cursor.execute(query, params)
        rows = cursor.fetchall()

        for row in rows:
            users.append(DWHUser(
                user_id=row.UserID,
                username=row.Username,
                role=row.Role,
                tenant_id=row.TenantID,
                tenant_name=row.TenantName,
                email=f"{row.Username}@dwh.local",
                is_active=bool(row.IsActive),
            ))

        cursor.close()
        conn.close()

        logger.info(
            "Fetched DWH users | total=%d | tenant_id=%s",
            len(users), tenant_id or "ALL"
        )

    except pyodbc.Error as ex:
        logger.error("Failed to fetch DWH users from SQL Server: %s", ex)
        raise ConnectionError(f"Database connection failed: {ex}") from ex

    return users


# ============================================================================
# Sync Logic
# ============================================================================

def sync_users_to_superset(
    dry_run: bool = False,
    tenant_id: Optional[str] = None,
    create_missing_roles: bool = False,
) -> SyncResult:
    """
    Sync DWH AppUsers to Superset users.

    Args:
        dry_run: If True, only show what would be done without making changes.
        tenant_id: Optional tenant ID to filter by. If None, sync all users.
        create_missing_roles: If True, create missing Superset roles automatically.

    Returns:
        SyncResult with counts and errors.
    """
    result = SyncResult()

    # Step 1: Fetch DWH users
    try:
        dwh_users = fetch_dwh_users(CONN_STR, tenant_id=tenant_id)
        result.total_users = len(dwh_users)
    except Exception as ex:
        result.errors.append(f"Failed to fetch DWH users: {ex}")
        result.failed = result.total_users
        return result

    if not dwh_users:
        logger.warning("No DWH users found to sync. tenant_id=%s", tenant_id)
        result.skipped = 0
        return result

    # Step 2: Connect to Superset
    try:
        client = SupersetClient(
            base_url=SUPERSET_URL,
            username=SUPERSET_ADMIN_USER,
            password=SUPERSET_ADMIN_PWD,
        )
    except Exception as ex:
        result.errors.append(f"Failed to connect to Superset: {ex}")
        return result

    # Step 3: Ensure required roles exist
    all_dwh_roles = set()
    for user in dwh_users:
        all_dwh_roles.update(user.get_superset_roles())

    if create_missing_roles:
        existing_roles = {r["name"] for r in client.get_roles()}
        for role_name in all_dwh_roles:
            if role_name not in existing_roles:
                logger.info("Would create role (dry_run=%s): %s", dry_run, role_name)

    # Step 4: Sync each user
    for user in dwh_users:
        try:
            superset_roles = user.get_superset_roles()
            extra_metadata = {
                "dwh_user_id": user.user_id,
                "dwh_role": user.role,
                "tenant_id": user.tenant_id or "",
                "tenant_name": user.tenant_name or "",
                "synced_at": datetime.utcnow().isoformat(),
                "source": "dwh_app_users",
            }

            if dry_run:
                logger.info(
                    "[DRY-RUN] Would sync user | username=%s | roles=%s | tenant=%s",
                    user.username, superset_roles, user.tenant_id or "N/A"
                )
                result.skipped += 1
                continue

            # Check if user already exists in Superset
            existing = client.get_user_by_username(user.username)

            if existing:
                # Update existing user
                client.update_user(
                    user_id=existing["id"],
                    roles=superset_roles,
                    extra=extra_metadata,
                    is_active=user.is_active,
                )
                result.updated += 1
                logger.info(
                    "Updated Superset user | username=%s | roles=%s",
                    user.username, superset_roles
                )
            else:
                # Create new user
                first_name, last_name = _split_name(user.username)
                client.create_user(
                    username=user.username,
                    email=user.email or f"{user.username}@dwh.local",
                    first_name=first_name,
                    last_name=last_name,
                    roles=superset_roles,
                    extra=extra_metadata,
                )
                result.created += 1
                logger.info(
                    "Created Superset user | username=%s | roles=%s",
                    user.username, superset_roles
                )

        except Exception as ex:
            result.failed += 1
            err_msg = f"User {user.username}: {ex}"
            result.errors.append(err_msg)
            logger.error("Failed to sync user %s: %s", user.username, ex)

    logger.info(
        "Sync complete | created=%d | updated=%d | skipped=%d | failed=%d",
        result.created, result.updated, result.skipped, result.failed
    )
    return result


def _split_name(username: str) -> tuple[str, str]:
    """
    Split a username into first name and last name components.

    Handles formats like: john_doe, john.doe, johndoe, john
    """
    # Handle underscore-separated names (john_doe)
    if "_" in username:
        parts = username.split("_", 1)
        return parts[0].capitalize(), parts[1].capitalize()

    # Handle dot-separated names (john.doe)
    if "." in username:
        parts = username.split(".", 1)
        return parts[0].capitalize(), parts[1].capitalize()

    # Handle camelCase (johnDoe)
    import re
    parts = re.split(r"(?=[A-Z])", username)
    if len(parts) >= 2:
        return parts[0].capitalize(), "".join(parts[1:]).capitalize()

    # Single word username
    return username.capitalize(), ""


# ============================================================================
# CLI Entry Point
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sync DWH AppUsers to Superset users",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--sync-all",
        action="store_true",
        help="Sync all users (default if no other filter is specified)",
    )
    parser.add_argument(
        "--sync-tenant",
        type=str,
        metavar="TENANT_ID",
        help="Sync only users for a specific tenant (e.g. STORE_HN)",
    )
    parser.add_argument(
        "--create-missing-roles",
        action="store_true",
        help="Automatically create missing Superset roles",
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
    logger.info("Superset User Sync — Phase 13")
    logger.info("=" * 60)

    if not CONN_STR:
        logger.error("CONN_STR environment variable is not set.")
        sys.exit(1)

    result = sync_users_to_superset(
        dry_run=args.dry_run,
        tenant_id=args.sync_tenant,
        create_missing_roles=args.create_missing_roles,
    )

    print("\n" + "=" * 60)
    print("SYNC RESULT")
    print("=" * 60)
    print(f"  Total users scanned : {result.total_users}")
    print(f"  Created             : {result.created}")
    print(f"  Updated             : {result.updated}")
    print(f"  Skipped (dry-run)   : {result.skipped}")
    print(f"  Failed              : {result.failed}")
    if result.errors:
        print(f"\n  ERRORS ({len(result.errors)}):")
        for err in result.errors:
            print(f"    - {err}")
    print("=" * 60)

    sys.exit(0 if result.failed == 0 else 1)


if __name__ == "__main__":
    main()