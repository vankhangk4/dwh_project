"""
superset/superset_client.py
Phase 13: Superset REST API client — reusable utility.

This module provides a well-typed SupersetClient class that wraps the
Superset REST API v1. It is used by:
    - superset/scripts/create_users.py
    - superset/scripts/create_roles_rls.py
    - superset/scripts/seed_dashboards.py
    - api/routes/superset.py (Phase 13)

Features:
    - Automatic token management (login, refresh).
    - Full CRUD for users, roles, RLS filters, dashboards, and datasources.
    - Guest token generation for embedded dashboards.
    - Typed responses and comprehensive error handling.
    - Logging with context.

Author: Nguyen Van Khang
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
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
SUPERSET_ADMIN_PWD = os.environ.get("SUPERSET_PASSWORD", "")
SUPERSET_TIMEOUT = int(os.environ.get("SUPERSET_TIMEOUT_SECONDS", "30"))

# Ensure venv site-packages is at front of sys.path for CLI usage.
_PROJ_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_VENV_SITE = os.path.join(_PROJ_ROOT, ".venv", "lib", "python3.13", "site-packages")
if _VENV_SITE not in sys.path:
    sys.path.insert(0, _VENV_SITE)


# ============================================================================
# Dataclasses
# ============================================================================

@dataclass
class SupersetUser:
    """Represents a Superset user."""
    id: int
    username: str
    email: str
    first_name: str
    last_name: str
    is_active: bool
    roles: list[str]
    extra: Optional[dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SupersetUser":
        role_names = [
            r.get("name", "") if isinstance(r, dict) else str(r)
            for r in data.get("roles", [])
        ]
        extra_str = data.get("extra", "{}")
        if isinstance(extra_str, str):
            try:
                extra = json.loads(extra_str)
            except (json.JSONDecodeError, TypeError):
                extra = {}
        else:
            extra = extra_str or {}

        return cls(
            id=data.get("id", 0),
            username=data.get("username", ""),
            email=data.get("email", ""),
            first_name=data.get("first_name", ""),
            last_name=data.get("last_name", ""),
            is_active=data.get("is_active", True),
            roles=role_names,
            extra=extra,
        )


@dataclass
class SupersetRole:
    """Represents a Superset role."""
    id: int
    name: str
    description: str
    permissions: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SupersetRole":
        return cls(
            id=data.get("id", 0),
            name=data.get("name", ""),
            description=data.get("description", ""),
            permissions=[
                p.get("name", "") if isinstance(p, dict) else str(p)
                for p in data.get("permissions", [])
            ],
        )


@dataclass
class RLSFilter:
    """Represents a Row-Level Security filter."""
    id: int
    clause: str
    role_id: int
    filter_type: str
    role_name: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RLSFilter":
        return cls(
            id=data.get("id", 0),
            clause=data.get("clause", ""),
            role_id=data.get("role_id", 0),
            filter_type=data.get("filter_type", "where"),
            role_name=data.get("role", {}).get("name") if isinstance(data.get("role"), dict) else None,
        )


@dataclass
class DashboardInfo:
    """Represents a Superset dashboard."""
    id: int
    dashboard_title: str
    slug: Optional[str]
    published: bool
    description: str
    url: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DashboardInfo":
        return cls(
            id=data.get("id", 0),
            dashboard_title=data.get("dashboard_title", ""),
            slug=data.get("slug"),
            published=data.get("published", False),
            description=data.get("description", ""),
            url=data.get("url"),
        )


@dataclass
class SupersetHealth:
    """Superset health check result."""
    status: str
    version: str
    database_ok: bool
    celery_active: bool

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SupersetHealth":
        return cls(
            status=data.get("status", "unknown"),
            version=data.get("version", ""),
            database_ok=data.get("database", {}).get("status") == "OK" if isinstance(data.get("database"), dict) else False,
            celery_active=data.get("celery", {}).get("status") == "OK" if isinstance(data.get("celery"), dict) else False,
        )


# ============================================================================
# SupersetClient
# ============================================================================

class SupersetClient:
    """
    Fully-featured client for Superset REST API v1.

    Provides typed methods for:
        - Authentication (login, refresh, guest token)
        - User management (create, read, update, delete)
        - Role management (create, assign permissions)
        - RLS filter management
        - Dashboard management
        - Datasource management
        - Health checks

    Example:
        client = SupersetClient(
            base_url="http://localhost:8088",
            username="admin",
            password="admin",
        )
        users = client.list_users()
        health = client.health_check()
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        timeout: int = 30,
    ):
        self.base_url = (base_url or SUPERSET_URL).rstrip("/")
        self.username = username or SUPERSET_ADMIN_USER
        self.password = password or SUPERSET_ADMIN_PWD
        self.timeout = timeout
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._token_issued_at: Optional[float] = None
        self._logged_in = False

        # Auto-login on construction
        try:
            self.login()
        except Exception as ex:
            logger.warning(
                "Auto-login to Superset failed on construction: %s. "
                "Call login() manually before making API calls.",
                ex
            )

    # --------------------------------------------------------------------------
    # Authentication
    # --------------------------------------------------------------------------

    def login(self) -> None:
        """
        Authenticate with Superset using username/password.

        Stores access and refresh tokens internally.
        Subsequent calls re-authenticate (refresh tokens expire).
        """
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
                timeout=self.timeout,
            )
        except requests.RequestException as ex:
            raise ConnectionError(
                f"Cannot connect to Superset at {self.base_url}: {ex}"
            ) from ex

        if resp.status_code == 401:
            raise PermissionError(
                f"Superset authentication failed for user '{self.username}'. "
                f"Check SUPERSET_USERNAME and SUPERSET_PASSWORD. "
                f"Response: {resp.text[:200]}"
            )
        elif resp.status_code != 200:
            raise RuntimeError(
                f"Superset login failed: HTTP {resp.status_code} — {resp.text[:200]}"
            )

        data = resp.json()
        self._access_token = data.get("access_token", "")
        self._refresh_token = data.get("refresh_token", "")
        self._token_issued_at = time.time()
        self._logged_in = True

        if not self._access_token:
            raise RuntimeError("Superset returned empty access token")

        logger.debug(
            "Superset login successful | user=%s | token_expires_in=%ds",
            self.username,
            data.get("expires_in", "N/A")
        )

    def refresh_token(self) -> None:
        """
        Refresh the access token using the refresh token.

        Falls back to full re-authentication if refresh fails.
        """
        if not self._refresh_token:
            self.login()
            return

        url = f"{self.base_url}/api/v1/security/refresh"
        try:
            resp = requests.post(
                url,
                headers={
                    "Authorization": f"Bearer {self._refresh_token}",
                    "Content-Type": "application/json",
                },
                timeout=self.timeout,
            )

            if resp.status_code == 401:
                logger.debug("Token refresh failed, re-authenticating...")
                self.login()
                return

            resp.raise_for_status()
            data = resp.json()
            self._access_token = data.get("access_token", "")
            self._token_issued_at = time.time()
            logger.debug("Superset token refreshed")

        except requests.RequestException as ex:
            logger.warning("Token refresh error: %s. Re-authenticating.", ex)
            self.login()

    def _headers(self) -> dict[str, str]:
        """Return HTTP headers with current access token."""
        if not self._logged_in or not self._access_token:
            self.login()
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
        retry_on_401: bool = True,
    ) -> requests.Response:
        """
        Make an authenticated HTTP request to Superset API.

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE).
            endpoint: API endpoint path (e.g. "/api/v1/users").
            data: JSON payload body.
            params: Query string parameters.
            retry_on_401: If True, re-authenticate and retry on 401.

        Returns:
            requests.Response object.

        Raises:
            requests.RequestException on unrecoverable HTTP error.
        """
        url = f"{self.base_url}{endpoint}"
        headers = self._headers()

        try:
            resp = requests.request(
                method,
                url,
                json=data,
                params=params,
                headers=headers,
                timeout=self.timeout,
            )

            if resp.status_code == 401 and retry_on_401:
                logger.debug("Got 401, refreshing token and retrying...")
                self.refresh_token()
                headers = self._headers()
                resp = requests.request(
                    method, url, json=data, params=params,
                    headers=headers, timeout=self.timeout,
                )

            return resp

        except requests.RequestException as ex:
            logger.error(
                "Superset API request failed | %s %s: %s",
                method, url, ex
            )
            raise

    # --------------------------------------------------------------------------
    # Health Check
    # --------------------------------------------------------------------------

    def health_check(self) -> SupersetHealth:
        """
        Check Superset health status.

        Returns:
            SupersetHealth object with status, version, and component health.
        """
        try:
            resp = self._request("GET", "/health", retry_on_401=False)
            resp.raise_for_status()
            return SupersetHealth.from_dict(resp.json())
        except requests.RequestException as ex:
            logger.error("Superset health check failed: %s", ex)
            return SupersetHealth(
                status="DOWN",
                version="",
                database_ok=False,
                celery_active=False,
            )

    def is_healthy(self) -> bool:
        """Quick check: is Superset responding?"""
        try:
            resp = requests.get(
                f"{self.base_url}/health",
                timeout=5,
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

    # --------------------------------------------------------------------------
    # User Management
    # --------------------------------------------------------------------------

    def list_users(
        self,
        page: int = 0,
        page_size: int = 100,
        filters: Optional[list[dict[str, Any]]] = None,
    ) -> list[SupersetUser]:
        """
        List users in Superset with optional filters.

        Args:
            page: Page number (0-indexed).
            page_size: Results per page.
            filters: Optional filter list, e.g.
                [{"col": "is_active", "opr": "eq", "value": True}]

        Returns:
            List of SupersetUser objects.
        """
        params: dict[str, Any] = {"page": page, "page_size": page_size}
        if filters:
            params["filters"] = json.dumps(filters)

        resp = self._request("GET", "/api/v1/users", params=params)
        resp.raise_for_status()
        payload = resp.json()

        users = []
        for item in payload.get("result", []):
            try:
                users.append(SupersetUser.from_dict(item))
            except Exception as ex:
                logger.warning("Failed to parse user item: %s", ex)

        return users

    def get_user_by_username(self, username: str) -> Optional[SupersetUser]:
        """Find a Superset user by username."""
        users = self.list_users(page_size=500)
        for user in users:
            if user.username == username:
                return user
        return None

    def get_user_by_email(self, email: str) -> Optional[SupersetUser]:
        """Find a Superset user by email."""
        users = self.list_users(
            filters=[{"col": "email", "opr": "eq", "value": email}]
        )
        return users[0] if users else None

    def create_user(
        self,
        username: str,
        email: str,
        first_name: str,
        last_name: str,
        roles: list[str],
        extra: Optional[dict[str, Any]] = None,
        password: Optional[str] = None,
    ) -> SupersetUser:
        """
        Create a new Superset user.

        Args:
            username: Unique username.
            email: Email address.
            first_name: First name.
            last_name: Last name.
            roles: List of Superset role names to assign.
            extra: Custom metadata dict (stored as JSON in Superset).
            password: Optional initial password (if Superset auth allows).

        Returns:
            Created SupersetUser object.

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
        if password:
            payload["password"] = password

        resp = self._request("POST", "/api/v1/users", data=payload)
        if resp.status_code not in (200, 201):
            body = resp.text
            if "already exists" in body.lower() or "unique" in body.lower():
                existing = self.get_user_by_username(username)
                if existing:
                    return existing
            raise RuntimeError(
                f"Failed to create user '{username}': "
                f"HTTP {resp.status_code} — {body[:300]}"
            )

        data = resp.json()
        result = data.get("result", {})
        logger.info(
            "Superset user created | username=%s | roles=%s",
            username, roles
        )
        return SupersetUser.from_dict(result)

    def update_user(
        self,
        user_id: int,
        roles: Optional[list[str]] = None,
        extra: Optional[dict[str, Any]] = None,
        is_active: Optional[bool] = None,
    ) -> SupersetUser:
        """
        Update an existing Superset user.

        Args:
            user_id: Superset user ID.
            roles: New list of Superset role names.
            extra: Custom metadata to update.
            is_active: Account active status.

        Returns:
            Updated SupersetUser object.
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
                f"Failed to update user {user_id}: HTTP {resp.status_code} — {resp.text[:300]}"
            )

        data = resp.json()
        result = data.get("result", {})
        return SupersetUser.from_dict(result)

    def delete_user(self, user_id: int) -> bool:
        """Delete a Superset user."""
        resp = self._request("DELETE", f"/api/v1/users/{user_id}")
        if resp.status_code not in (200, 204):
            logger.warning("Failed to delete user %d: %s", user_id, resp.text[:100])
            return False
        logger.info("Superset user deleted | user_id=%d", user_id)
        return True

    def get_or_create_user(
        self,
        username: str,
        email: str,
        first_name: str,
        last_name: str,
        roles: list[str],
        extra: Optional[dict[str, Any]] = None,
    ) -> tuple[SupersetUser, bool]:
        """
        Get existing user or create if not exists.

        Args:
            username: Username to look up or create.
            email: Email address.
            first_name: First name.
            last_name: Last name.
            roles: Superset role names.
            extra: Custom metadata.

        Returns:
            Tuple of (SupersetUser, is_new: bool).
        """
        existing = self.get_user_by_username(username)
        if existing:
            if roles is not None:
                self.update_user(existing.id, roles=roles, extra=extra)
            return existing, False
        created = self.create_user(username, email, first_name, last_name, roles, extra)
        return created, True

    # --------------------------------------------------------------------------
    # Role Management
    # --------------------------------------------------------------------------

    def list_roles(self, page_size: int = 500) -> list[SupersetRole]:
        """List all Superset roles."""
        resp = self._request("GET", "/api/v1/roles", params={"page_size": page_size})
        resp.raise_for_status()
        payload = resp.json()
        roles = []
        for item in payload.get("result", []):
            try:
                roles.append(SupersetRole.from_dict(item))
            except Exception as ex:
                logger.warning("Failed to parse role item: %s", ex)
        return roles

    def get_role_by_name(self, name: str) -> Optional[SupersetRole]:
        """Find a Superset role by name."""
        roles = self.list_roles()
        for role in roles:
            if role.name == name:
                return role
        return None

    def create_role(
        self,
        name: str,
        description: str = "",
    ) -> SupersetRole:
        """
        Create a new Superset role.

        Args:
            name: Role name (must be unique).
            description: Role description.

        Returns:
            Created SupersetRole object.
        """
        existing = self.get_role_by_name(name)
        if existing:
            logger.debug("Role '%s' already exists, returning existing.", name)
            return existing

        payload = {"name": name, "description": description}
        resp = self._request("POST", "/api/v1/roles", data=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to create role '{name}': HTTP {resp.status_code} — {resp.text[:200]}"
            )

        data = resp.json()
        result = data.get("result", {})
        logger.info("Superset role created | name=%s", name)
        return SupersetRole.from_dict(result)

    def delete_role(self, role_id: int) -> bool:
        """Delete a Superset role."""
        resp = self._request("DELETE", f"/api/v1/roles/{role_id}")
        if resp.status_code not in (200, 204):
            logger.warning("Failed to delete role %d: %s", role_id, resp.text[:100])
            return False
        logger.info("Superset role deleted | role_id=%d", role_id)
        return True

    def add_permission_to_role(self, role_id: int, permission_name: str) -> bool:
        """
        Add a named permission to a role.

        Args:
            role_id: Target role ID.
            permission_name: Permission name (e.g. "can_explore_json").

        Returns:
            True if added or already exists, False if permission not found.
        """
        # Step 1: Find the permission
        resp = self._request("GET", "/api/v1/permissions", retry_on_401=False)
        resp.raise_for_status()
        permissions = resp.json().get("result", [])

        perm_id = None
        for p in permissions:
            if p.get("name") == permission_name:
                perm_id = p.get("id")
                break

        if perm_id is None:
            logger.warning("Permission '%s' not found in Superset", permission_name)
            return False

        # Step 2: Add permission to role
        resp = self._request(
            "POST",
            f"/api/v1/roles/{role_id}/permissions",
            data={"permission_id": perm_id},
        )
        if resp.status_code not in (200, 201):
            logger.debug(
                "Permission '%s' add may have failed for role %d: %s",
                permission_name, role_id, resp.text[:100]
            )
        return True

    # --------------------------------------------------------------------------
    # RLS Filter Management
    # --------------------------------------------------------------------------

    def list_rls_filters(self) -> list[RLSFilter]:
        """List all Row-Level Security filters."""
        resp = self._request("GET", "/api/v1/rls")
        resp.raise_for_status()
        payload = resp.json()
        filters = []
        for item in payload.get("result", []):
            try:
                filters.append(RLSFilter.from_dict(item))
            except Exception as ex:
                logger.warning("Failed to parse RLS filter item: %s", ex)
        return filters

    def create_rls_filter(
        self,
        clause: str,
        role_id: int,
        filter_type: str = "where",
    ) -> RLSFilter:
        """
        Create a Row-Level Security filter.

        Args:
            clause: SQL WHERE clause (e.g. "TenantID = 'STORE_HN'").
            role_id: Superset role ID to apply the filter to.
            filter_type: Filter type — "where" or "having".

        Returns:
            Created RLSFilter object.
        """
        # Check for duplicate
        existing_filters = self.list_rls_filters()
        for f in existing_filters:
            if f.clause == clause and f.role_id == role_id:
                logger.debug(
                    "RLS filter already exists | clause='%s' role_id=%d",
                    clause, role_id
                )
                return f

        payload = {
            "clause": clause,
            "role_id": role_id,
            "filter_type": filter_type,
        }
        resp = self._request("POST", "/api/v1/rls", data=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to create RLS filter: HTTP {resp.status_code} — {resp.text[:200]}"
            )

        data = resp.json()
        result = data.get("result", {})
        logger.info(
            "RLS filter created | clause='%s' role_id=%d type=%s",
            clause, role_id, filter_type
        )
        return RLSFilter.from_dict(result)

    def delete_rls_filter(self, filter_id: int) -> bool:
        """Delete a Row-Level Security filter."""
        resp = self._request("DELETE", f"/api/v1/rls/{filter_id}")
        if resp.status_code not in (200, 204):
            logger.warning("Failed to delete RLS filter %d: %s", filter_id, resp.text[:100])
            return False
        logger.info("RLS filter deleted | filter_id=%d", filter_id)
        return True

    def get_rls_filters_for_role(self, role_id: int) -> list[RLSFilter]:
        """Get all RLS filters applied to a specific role."""
        all_filters = self.list_rls_filters()
        return [f for f in all_filters if f.role_id == role_id]

    # --------------------------------------------------------------------------
    # Dashboard Management
    # --------------------------------------------------------------------------

    def list_dashboards(self, page_size: int = 100) -> list[DashboardInfo]:
        """List all Superset dashboards."""
        resp = self._request("GET", "/api/v1/dashboard", params={"page_size": page_size})
        resp.raise_for_status()
        payload = resp.json()
        dashboards = []
        for item in payload.get("result", []):
            try:
                dashboards.append(DashboardInfo.from_dict(item))
            except Exception as ex:
                logger.warning("Failed to parse dashboard item: %s", ex)
        return dashboards

    def get_dashboard_by_title(self, title: str) -> Optional[DashboardInfo]:
        """Find a dashboard by title."""
        dashboards = self.list_dashboards()
        for d in dashboards:
            if d.dashboard_title == title:
                return d
        return None

    def create_dashboard(
        self,
        title: str,
        description: str = "",
        owners: Optional[list[int]] = None,
        published: bool = True,
        slug: Optional[str] = None,
        json_metadata: Optional[dict[str, Any]] = None,
        css: Optional[str] = None,
        position_json: Optional[dict[str, Any]] = None,
    ) -> DashboardInfo:
        """
        Create a new Superset dashboard.

        Args:
            title: Dashboard title.
            description: Dashboard description.
            owners: List of user IDs who can manage this dashboard.
            published: Whether the dashboard is visible to users with access.
            slug: URL slug for the dashboard.
            json_metadata: Dashboard metadata dict.
            css: Custom CSS for the dashboard.
            position_json: Dashboard layout/position data.

        Returns:
            Created DashboardInfo object.
        """
        if owners is None:
            owners = []

        payload: dict[str, Any] = {
            "dashboard_title": title,
            "description": description,
            "owners": owners,
            "published": published,
        }
        if slug:
            payload["slug"] = slug
        if json_metadata:
            payload["json_metadata"] = json.dumps(json_metadata)
        if css:
            payload["css"] = css
        if position_json:
            payload["position_json"] = json.dumps(position_json)

        resp = self._request("POST", "/api/v1/dashboard", data=payload)
        if resp.status_code not in (200, 201):
            # Check for duplicate
            if "already exists" in resp.text.lower():
                existing = self.get_dashboard_by_title(title)
                if existing:
                    return existing
            raise RuntimeError(
                f"Failed to create dashboard '{title}': "
                f"HTTP {resp.status_code} — {resp.text[:200]}"
            )

        data = resp.json()
        result = data.get("result", {})
        logger.info("Dashboard created | title='%s' id=%s", title, result.get("id"))
        return DashboardInfo.from_dict(result)

    def update_dashboard(
        self,
        dashboard_id: int,
        **kwargs: Any,
    ) -> DashboardInfo:
        """Update an existing Superset dashboard fields."""
        resp = self._request("PUT", f"/api/v1/dashboard/{dashboard_id}", data=kwargs)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to update dashboard {dashboard_id}: {resp.text[:200]}"
            )
        data = resp.json()
        return DashboardInfo.from_dict(data.get("result", {}))

    def get_dashboard_url(self, dashboard_id: int, guest_token: Optional[str] = None) -> str:
        """
        Build the URL for a dashboard.

        Args:
            dashboard_id: Dashboard ID.
            guest_token: Optional guest token for embedded access.

        Returns:
            Full dashboard URL.
        """
        url = f"{self.base_url}/superset/dashboard/{dashboard_id}/"
        if guest_token:
            url += f"?guest_token={guest_token}"
        return url

    # --------------------------------------------------------------------------
    # Guest Token
    # --------------------------------------------------------------------------

    def create_guest_token(
        self,
        username: str,
        roles: list[str],
        resources: Optional[list[dict[str, Any]]] = None,
        rls: Optional[list[dict[str, Any]]] = None,
        expiry_seconds: int = 3600,
    ) -> tuple[str, int]:
        """
        Create a guest token for embedded dashboard access.

        Args:
            username: Username to impersonate.
            roles: Superset roles to grant (e.g. ["Gamma"]).
            resources: List of resources to expose, e.g.
                [{"type": "dashboard", "id": 1}]
            rls: Row-level security filters to apply, e.g.
                [{"clause": "TenantID = 'STORE_HN'"}]
            expiry_seconds: Token expiry in seconds.

        Returns:
            Tuple of (guest_token, expiry_seconds).
        """
        if resources is None:
            resources = [{"type": "dashboard", "id": 1}]
        if rls is None:
            rls = []

        payload: dict[str, Any] = {
            "user": {
                "username": username,
                "roles": roles,
            },
            "resources": resources,
            "rls": rls,
        }

        resp = self._request("POST", "/api/v1/security/guest_token", data=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Guest token creation failed: HTTP {resp.status_code} — {resp.text[:200]}"
            )

        data = resp.json()
        token = data.get("token", "")
        if not token:
            raise RuntimeError("No guest token returned from Superset")

        logger.info(
            "Guest token created | user=%s | roles=%s | resources=%d | rls=%d",
            username, roles, len(resources), len(rls)
        )
        return token, expiry_seconds

    def create_guest_token_for_dashboard(
        self,
        username: str,
        dashboard_id: int,
        tenant_id: Optional[str] = None,
        roles: Optional[list[str]] = None,
        expiry_seconds: int = 3600,
    ) -> tuple[str, int, str]:
        """
        Create a guest token for a specific dashboard with optional RLS.

        This is the primary method used by the Auth Gateway to embed dashboards.

        Args:
            username: Username for the guest token.
            dashboard_id: Target dashboard ID.
            tenant_id: Tenant ID for RLS filter (e.g. "STORE_HN").
            roles: Superset roles (default: ["Gamma"]).
            expiry_seconds: Token expiry.

        Returns:
            Tuple of (guest_token, expiry_seconds, dashboard_url).
        """
        if roles is None:
            roles = ["Gamma"]

        resources = [{"type": "dashboard", "id": str(dashboard_id)}]
        rls: list[dict[str, Any]] = []
        if tenant_id:
            rls.append({"clause": f"TenantID = '{tenant_id}'"})

        token, expires_in = self.create_guest_token(
            username=username,
            roles=roles,
            resources=resources,
            rls=rls,
            expiry_seconds=expiry_seconds,
        )

        url = self.get_dashboard_url(dashboard_id, guest_token=token)
        return token, expires_in, url

    # --------------------------------------------------------------------------
    # Datasource Management
    # --------------------------------------------------------------------------

    def list_datasources(self, page_size: int = 100) -> list[dict[str, Any]]:
        """List all registered datasources."""
        resp = self._request("GET", "/api/v1/dataset", params={"page_size": page_size})
        resp.raise_for_status()
        return resp.json().get("result", [])

    def get_datasource_by_name(
        self,
        table_name: str,
        schema: str = "dbo",
    ) -> Optional[dict[str, Any]]:
        """Find a datasource by table name and schema."""
        datasources = self.list_datasources()
        for ds in datasources:
            if (ds.get("table_name") == table_name and
                    ds.get("schema") == schema):
                return ds
        return None

    def create_database(
        self,
        database_name: str,
        sqlalchemy_uri: str,
        extra: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Register a new database in Superset.

        Args:
            database_name: Display name for the database.
            sqlalchemy_uri: SQLAlchemy connection URI.
            extra: Additional database configuration.

        Returns:
            Created database dict.
        """
        payload: dict[str, Any] = {
            "database_name": database_name,
            "sqlalchemy_uri": sqlalchemy_uri,
        }
        if extra:
            payload["extra"] = json.dumps(extra)

        resp = self._request("POST", "/api/v1/database", data=payload)
        if resp.status_code not in (200, 201):
            raise RuntimeError(
                f"Failed to create database '{database_name}': "
                f"HTTP {resp.status_code} — {resp.text[:200]}"
            )

        data = resp.json()
        logger.info("Superset database created | name=%s", database_name)
        return data.get("result", {})

    # --------------------------------------------------------------------------
    # Bulk Operations
    # --------------------------------------------------------------------------

    def provision_tenant(
        self,
        tenant_id: str,
        tenant_name: str,
        owner_user_ids: Optional[list[int]] = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """
        Full tenant provisioning in Superset.

        Creates:
            1. Tenant-specific RLS role: "Tenant_{tenant_id}"
            2. RLS filter: "TenantID = '{tenant_id}'"
            3. Assigns permissions to the role

        Args:
            tenant_id: Tenant ID from DWH (e.g. "STORE_HN").
            tenant_name: Tenant display name.
            owner_user_ids: User IDs who can manage the role.
            dry_run: If True, show what would be created.

        Returns:
            Dict with provisioning results.
        """
        role_name = f"Tenant_{tenant_id.upper().replace('-', '_').replace(' ', '_')}"
        rls_clause = f"TenantID = '{tenant_id}'"
        result: dict[str, Any] = {
            "tenant_id": tenant_id,
            "role_name": role_name,
            "role_id": None,
            "rls_filter_id": None,
            "status": "DRY-RUN" if dry_run else "SUCCESS",
            "errors": [],
        }

        if dry_run:
            logger.info(
                "[DRY-RUN] Would provision tenant | tenant=%s | role=%s | rls='%s'",
                tenant_id, role_name, rls_clause
            )
            return result

        try:
            # Create role
            role = self.create_role(
                name=role_name,
                description=f"Auto-provisioned role for tenant: {tenant_name} ({tenant_id})",
            )
            result["role_id"] = role.id

            # Grant read permissions
            read_perms = [
                "menu_access[SQL Lab]",
                "menu_access[Dashboards]",
                "can_explore_json",
                "can_explore",
                "can_list_dashboards",
                "can_sqllab_viz",
            ]
            for perm in read_perms:
                self.add_permission_to_role(role.id, perm)

            # Create RLS filter
            rls = self.create_rls_filter(clause=rls_clause, role_id=role.id)
            result["rls_filter_id"] = rls.id

            logger.info(
                "Tenant provisioned | tenant=%s | role=%s | rls_id=%s",
                tenant_id, role_name, rls.id
            )

        except Exception as ex:
            result["status"] = "ERROR"
            result["errors"].append(str(ex))
            logger.error("Tenant provisioning failed for %s: %s", tenant_id, ex)

        return result

    def deprovision_tenant(self, tenant_id: str) -> dict[str, Any]:
        """
        Remove a tenant's Superset role and RLS filters.

        Args:
            tenant_id: Tenant ID to remove.

        Returns:
            Dict with deprovisioning results.
        """
        role_name = f"Tenant_{tenant_id.upper().replace('-', '_').replace(' ', '_')}"
        result: dict[str, Any] = {
            "tenant_id": tenant_id,
            "role_deleted": False,
            "rls_deleted": 0,
            "errors": [],
        }

        role = self.get_role_by_name(role_name)
        if not role:
            logger.info("Tenant role '%s' not found, nothing to remove.", role_name)
            return result

        # Delete RLS filters
        rls_filters = self.get_rls_filters_for_role(role.id)
        for rls in rls_filters:
            if self.delete_rls_filter(rls.id):
                result["rls_deleted"] += 1

        # Delete role
        if self.delete_role(role.id):
            result["role_deleted"] = True

        return result

    def __repr__(self) -> str:
        return (
            f"SupersetClient(url={self.base_url}, user={self.username}, "
            f"logged_in={self._logged_in})"
        )
