#!/usr/bin/env python3
# ============================================================================
# superset/scripts/seed_dashboards.py
# Phase 13: Script to create 5 dashboard scaffolds in Superset.
#
# This script creates the initial dashboard structure for the DWH
# multi-tenant analytics portal.
#
# Dashboards created:
#   1. Sales Overview        (id=1)  — KPI cards, revenue trends, top products
#   2. Inventory Management  (id=2)  — Stock levels, reorder alerts, turnover
#   3. Customer Analytics    (id=3)  — RFM segments, churn, loyalty points
#   4. Employee Performance  (id=4)  — Sales by employee, rankings, KPIs
#   5. Purchase Overview     (id=5)  — Supplier spend, PO trends, costs
#
# Each dashboard includes:
#   - Pre-configured charts pointing to the Data Mart tables
#   - Proper dashboard roles and ownership
#   - Positioned chart grid
#   - Owner set to the admin user
#
# Usage:
#   python superset/scripts/seed_dashboards.py --dry-run
#   python superset/scripts/seed_dashboards.py --create-all
#   python superset/scripts/seed_dashboards.py --dashboard 1
#   python superset/scripts/seed_dashboards.py --list
#
# Prerequisites:
#   - Superset must be running and accessible.
#   - Admin credentials configured in .env.
#   - SQL Server datasource must be registered in Superset first.
#
# Author: Nguyen Van Khang
# ============================================================================

from __future__ import annotations

import argparse
import json
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

import requests

# Import SupersetClient from create_users if available
sys.path.insert(0, os.path.dirname(__file__))
try:
    from create_users import SupersetClient
except ImportError:
    SupersetClient = None

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://localhost:8088").rstrip("/")
SUPERSET_ADMIN_USER = os.environ.get("SUPERSET_USERNAME", "admin")
SUPERSET_ADMIN_PWD = os.environ.get("SUPERSET_PASSWORD", "admin")

# Default admin user ID in Superset (usually 1 for the first admin)
DEFAULT_OWNER_USER_ID = 1


# ============================================================================
# Superset Client (inline fallback)
# ============================================================================

if SupersetClient is None:
    class SupersetClient:
        def __init__(self, base_url: str, username: str, password: str):
            self.base_url = base_url.rstrip("/")
            self.username = username
            self.password = password
            self._access_token: Optional[str] = None
            self._refresh_token: Optional[str] = None
            self._login()

        def _login(self) -> None:
            url = f"{self.base_url}/api/v1/security/login"
            payload = {"username": self.username, "password": self.password,
                       "provider": "db", "refresh": True}
            resp = requests.post(url, json=payload,
                                 headers={"Content-Type": "application/json"}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            self._access_token = data.get("access_token", "")
            self._refresh_token = data.get("refresh_token", "")
            if not self._access_token:
                raise RuntimeError("No access token from Superset")

        def _headers(self) -> dict[str, str]:
            return {"Authorization": f"Bearer {self._access_token}",
                    "Content-Type": "application/json"}

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

        def list_dashboards(self) -> list[dict[str, Any]]:
            resp = self._request("GET", "/api/v1/dashboard",
                                  params={"page_size": 100})
            resp.raise_for_status()
            return resp.json().get("result", [])

        def get_dashboard_by_title(self, title: str) -> Optional[dict[str, Any]]:
            for d in self.list_dashboards():
                if d.get("dashboard_title") == title:
                    return d
            return None

        def create_dashboard(
            self,
            dashboard_id: int,
            title: str,
            description: str,
            slices: Optional[list[int]] = None,
            owners: Optional[list[int]] = None,
            css: Optional[str] = None,
            json_metadata: Optional[dict[str, Any]] = None,
            published: bool = True,
        ) -> dict[str, Any]:
            """Create a new dashboard."""
            if owners is None:
                owners = [DEFAULT_OWNER_USER_ID]
            if slices is None:
                slices = []

            payload: dict[str, Any] = {
                "dashboard_title": title,
                "description": description,
                "owners": owners,
                "published": published,
                "css": css or "",
                "position_json": json.dumps(self._default_position()),
            }

            if json_metadata:
                payload["json_metadata"] = json.dumps(json_metadata)

            resp = self._request("POST", "/api/v1/dashboard", data=payload)
            if resp.status_code not in (200, 201):
                # Check if already exists
                if "already exists" in resp.text.lower() or "unique" in resp.text.lower():
                    existing = self.get_dashboard_by_title(title)
                    if existing:
                        logger.debug("Dashboard '%s' already exists (id=%d)", title, existing["id"])
                        return existing
                raise RuntimeError(
                    f"Failed to create dashboard '{title}': HTTP {resp.status_code} — {resp.text[:200]}"
                )

            data = resp.json()
            result = data.get("result", {})
            logger.info("Dashboard created | title='%s' | id=%s", title, result.get("id"))
            return result

        def update_dashboard(self, dashboard_id: int,
                             **kwargs) -> dict[str, Any]:
            """Update an existing dashboard."""
            resp = self._request("PUT", f"/api/v1/dashboard/{dashboard_id}", data=kwargs)
            if resp.status_code not in (200, 201):
                raise RuntimeError(
                    f"Failed to update dashboard {dashboard_id}: {resp.text[:200]}"
                )
            return resp.json().get("result", {})

        @staticmethod
        def _default_position() -> dict[str, Any]:
            """Return a default 12-column grid position layout."""
            return {
                "root": "ROOT",
                "children": [
                    {
                        "type": "ROW",
                        "id": "ROW-1",
                        "children": [
                            {
                                "type": "CHART",
                                "id": "CHART-1",
                                "meta": {"width": 4, "height": 50},
                            },
                            {
                                "type": "CHART",
                                "id": "CHART-2",
                                "meta": {"width": 4, "height": 50},
                            },
                            {
                                "type": "CHART",
                                "id": "CHART-3",
                                "meta": {"width": 4, "height": 50},
                            },
                        ],
                    },
                    {
                        "type": "ROW",
                        "id": "ROW-2",
                        "children": [
                            {
                                "type": "CHART",
                                "id": "CHART-4",
                                "meta": {"width": 6, "height": 80},
                            },
                            {
                                "type": "CHART",
                                "id": "CHART-5",
                                "meta": {"width": 6, "height": 80},
                            },
                        ],
                    },
                ],
                "chartId": 0,
            }


# ============================================================================
# Dashboard Definitions
# ============================================================================

DASHBOARD_DEFINITIONS: list[dict[str, Any]] = [
    {
        "id": 1,
        "title": "Sales Overview",
        "description": (
            "Real-time sales performance dashboard. "
            "Monitor revenue, gross profit, order counts, and top-selling products "
            "across all stores and time periods."
        ),
        "slug": "sales-overview",
        "color_scheme": "blue",
        "charts": [
            {
                "name": "Total Revenue KPI",
                "viz_type": "big_number_total",
                "datasource_type": "table",
                "datasource_name": "DM_SalesSummary",
                "metrics": [{"expression": "SUM(TotalRevenue)", "label": "Total Revenue"}],
                "adhoc_filters": [],
                "position": {"row": 1, "col": 1, "size_x": 4, "size_y": 1},
            },
            {
                "name": "Total Profit KPI",
                "viz_type": "big_number_total",
                "datasource_type": "table",
                "datasource_name": "DM_SalesSummary",
                "metrics": [{"expression": "SUM(TotalProfit)", "label": "Total Profit"}],
                "adhoc_filters": [],
                "position": {"row": 1, "col": 5, "size_x": 4, "size_y": 1},
            },
            {
                "name": "Total Orders KPI",
                "viz_type": "big_number_total",
                "datasource_type": "table",
                "datasource_name": "DM_SalesSummary",
                "metrics": [{"expression": "SUM(TotalOrders)", "label": "Total Orders"}],
                "adhoc_filters": [],
                "position": {"row": 1, "col": 9, "size_x": 4, "size_y": 1},
            },
            {
                "name": "Revenue Trend (Line Chart)",
                "viz_type": "line",
                "datasource_type": "table",
                "datasource_name": "DM_SalesSummary",
                "metrics": [{"expression": "SUM(TotalRevenue)", "label": "Revenue"}],
                "groupby": ["DateKey"],
                "adhoc_filters": [],
                "position": {"row": 2, "col": 1, "size_x": 6, "size_y": 3},
            },
            {
                "name": "Sales by Category (Pie)",
                "viz_type": "pie",
                "datasource_type": "table",
                "datasource_name": "DM_SalesSummary",
                "metrics": [{"expression": "SUM(TotalRevenue)", "label": "Revenue"}],
                "groupby": ["CategoryName"],
                "adhoc_filters": [],
                "position": {"row": 2, "col": 7, "size_x": 6, "size_y": 3},
            },
        ],
        "metadata": {
            "filter_scopes": {},
            "default_filters": "{}",
            "refresh_frequency": 300,
            "timed_refresh_immune_slices": [],
        },
    },
    {
        "id": 2,
        "title": "Inventory Management",
        "description": (
            "Track inventory levels, reorder alerts, and stock turnover. "
            "Identify slow-moving items and optimize stock across stores."
        ),
        "slug": "inventory-management",
        "color_scheme": "green",
        "charts": [
            {
                "name": "Low Stock Alerts",
                "viz_type": "table",
                "datasource_type": "table",
                "datasource_name": "DM_InventoryAlert",
                "metrics": [
                    {"expression": "COUNT(*)", "label": "Alert Count"},
                    {"expression": "SUM(CurrentQty)", "label": "Current Stock"},
                ],
                "groupby": ["AlertLevel", "ProductKey"],
                "adhoc_filters": [
                    {"col": "AlertLevel", "op": "IN", "val": ["HIGH", "MEDIUM"]}
                ],
                "position": {"row": 1, "col": 1, "size_x": 12, "size_y": 2},
            },
            {
                "name": "Stock by Store (Bar)",
                "viz_type": "bar",
                "datasource_type": "table",
                "datasource_name": "FactInventory",
                "metrics": [{"expression": "SUM(ClosingQty)", "label": "Closing Qty"}],
                "groupby": ["StoreKey"],
                "adhoc_filters": [],
                "position": {"row": 2, "col": 1, "size_x": 6, "size_y": 3},
            },
            {
                "name": "Days of Stock Histogram",
                "viz_type": "histogram",
                "datasource_type": "table",
                "datasource_name": "DM_InventoryAlert",
                "metrics": [{"expression": "AVG(DaysOfStock)", "label": "Avg Days of Stock"}],
                "adhoc_filters": [],
                "position": {"row": 2, "col": 7, "size_x": 6, "size_y": 3},
            },
        ],
        "metadata": {
            "filter_scopes": {},
            "default_filters": "{}",
            "refresh_frequency": 600,
            "timed_refresh_immune_slices": [],
        },
    },
    {
        "id": 3,
        "title": "Customer Analytics",
        "description": (
            "Customer segmentation and behavior analysis. "
            "RFM analysis, loyalty tracking, and customer churn indicators."
        ),
        "slug": "customer-analytics",
        "color_scheme": "purple",
        "charts": [
            {
                "name": "RFM Segment Distribution",
                "viz_type": "pie",
                "datasource_type": "table",
                "datasource_name": "DM_CustomerRFM",
                "metrics": [{"expression": "COUNT(*)", "label": "Customer Count"}],
                "groupby": ["Segment"],
                "adhoc_filters": [],
                "position": {"row": 1, "col": 1, "size_x": 6, "size_y": 3},
            },
            {
                "name": "Avg Monetary Value by Segment",
                "viz_type": "bar",
                "datasource_type": "table",
                "datasource_name": "DM_CustomerRFM",
                "metrics": [{"expression": "AVG(MonetaryAmount)", "label": "Avg Monetary"}],
                "groupby": ["Segment"],
                "adhoc_filters": [],
                "position": {"row": 1, "col": 7, "size_x": 6, "size_y": 3},
            },
            {
                "name": "RFM Score Heatmap",
                "viz_type": "heatmap",
                "datasource_type": "table",
                "datasource_name": "DM_CustomerRFM",
                "metrics": [{"expression": "COUNT(*)", "label": "Customer Count"}],
                "groupby": ["RecencyDays", "RFMScore"],
                "adhoc_filters": [],
                "position": {"row": 2, "col": 1, "size_x": 12, "size_y": 3},
            },
        ],
        "metadata": {
            "filter_scopes": {},
            "default_filters": "{}",
            "refresh_frequency": 3600,
            "timed_refresh_immune_slices": [],
        },
    },
    {
        "id": 4,
        "title": "Employee Performance",
        "description": (
            "Sales representative performance metrics. "
            "Revenue per employee, order counts, average basket size, and rankings."
        ),
        "slug": "employee-performance",
        "color_scheme": "orange",
        "charts": [
            {
                "name": "Revenue by Employee",
                "viz_type": "bar",
                "datasource_type": "table",
                "datasource_name": "DM_EmployeePerformance",
                "metrics": [{"expression": "SUM(TotalRevenue)", "label": "Total Revenue"}],
                "groupby": ["EmployeeKey"],
                "adhoc_filters": [],
                "position": {"row": 1, "col": 1, "size_x": 8, "size_y": 3},
            },
            {
                "name": "Avg Basket Value KPI",
                "viz_type": "big_number_total",
                "datasource_type": "table",
                "datasource_name": "DM_EmployeePerformance",
                "metrics": [{"expression": "AVG(AvgBasketValue)", "label": "Avg Basket"}],
                "adhoc_filters": [],
                "position": {"row": 1, "col": 9, "size_x": 4, "size_y": 1},
            },
            {
                "name": "Orders Trend by Employee",
                "viz_type": "line",
                "datasource_type": "table",
                "datasource_name": "DM_EmployeePerformance",
                "metrics": [{"expression": "SUM(TotalOrders)", "label": "Orders"}],
                "groupby": ["DateKey", "EmployeeKey"],
                "adhoc_filters": [],
                "position": {"row": 2, "col": 1, "size_x": 12, "size_y": 3},
            },
        ],
        "metadata": {
            "filter_scopes": {},
            "default_filters": "{}",
            "refresh_frequency": 1800,
            "timed_refresh_immune_slices": [],
        },
    },
    {
        "id": 5,
        "title": "Purchase Overview",
        "description": (
            "Procurement and supplier management dashboard. "
            "Total purchase costs, supplier performance, and order tracking."
        ),
        "slug": "purchase-overview",
        "color_scheme": "red",
        "charts": [
            {
                "name": "Total Purchase Cost KPI",
                "viz_type": "big_number_total",
                "datasource_type": "table",
                "datasource_name": "DM_PurchaseSummary",
                "metrics": [{"expression": "SUM(TotalPurchaseCost)", "label": "Total Cost"}],
                "adhoc_filters": [],
                "position": {"row": 1, "col": 1, "size_x": 4, "size_y": 1},
            },
            {
                "name": "Total POs KPI",
                "viz_type": "big_number_total",
                "datasource_type": "table",
                "datasource_name": "DM_PurchaseSummary",
                "metrics": [{"expression": "SUM(TotalOrders)", "label": "Total Orders"}],
                "adhoc_filters": [],
                "position": {"row": 1, "col": 5, "size_x": 4, "size_y": 1},
            },
            {
                "name": "Purchase Cost by Supplier",
                "viz_type": "bar",
                "datasource_type": "table",
                "datasource_name": "DM_PurchaseSummary",
                "metrics": [{"expression": "SUM(TotalPurchaseCost)", "label": "Cost"}],
                "groupby": ["SupplierKey"],
                "adhoc_filters": [],
                "position": {"row": 2, "col": 1, "size_x": 6, "size_y": 3},
            },
            {
                "name": "Purchase Trend (Line)",
                "viz_type": "line",
                "datasource_type": "table",
                "datasource_name": "DM_PurchaseSummary",
                "metrics": [{"expression": "SUM(TotalPurchaseCost)", "label": "Cost"}],
                "groupby": ["DateKey"],
                "adhoc_filters": [],
                "position": {"row": 2, "col": 7, "size_x": 6, "size_y": 3},
            },
        ],
        "metadata": {
            "filter_scopes": {},
            "default_filters": "{}",
            "refresh_frequency": 3600,
            "timed_refresh_immune_slices": [],
        },
    },
]


# ============================================================================
# Dashboard Seeding Logic
# ============================================================================

def list_dashboards_in_superset(client: SupersetClient) -> list[dict[str, Any]]:
    """List all dashboards currently in Superset."""
    dashboards = client.list_dashboards()
    print("\n" + "=" * 60)
    print("CURRENT DASHBOARDS IN SUPERSET")
    print("=" * 60)
    if not dashboards:
        print("  (none)")
    for d in dashboards:
        print(f"  [{d.get('id')}] {d.get('dashboard_title')} — "
              f"published={d.get('published', False)}")
    print("=" * 60)
    return dashboards


def create_dashboard_scaffold(
    client: SupersetClient,
    definition: dict[str, Any],
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Create a single dashboard scaffold from a definition.

    Args:
        client: SupersetClient instance.
        definition: Dashboard definition dict from DASHBOARD_DEFINITIONS.
        dry_run: If True, only show what would be created.

    Returns:
        Dict with creation result.
    """
    dashboard_id = definition["id"]
    title = definition["title"]
    description = definition["description"]

    # Check if dashboard already exists
    existing = client.get_dashboard_by_title(title)
    if existing:
        existing_id = existing.get("id")
        logger.info(
            "Dashboard '%s' already exists in Superset (id=%d). "
            "Use --update flag to refresh.",
            title, existing_id
        )
        return {
            "dashboard_id": dashboard_id,
            "title": title,
            "status": "EXISTS",
            "superset_id": existing_id,
        }

    if dry_run:
        logger.info(
            "[DRY-RUN] Would create dashboard | id=%d | title='%s' | charts=%d",
            dashboard_id, title, len(definition.get("charts", []))
        )
        return {
            "dashboard_id": dashboard_id,
            "title": title,
            "status": "DRY-RUN",
            "charts_count": len(definition.get("charts", [])),
        }

    # Create dashboard
    dashboard = client.create_dashboard(
        dashboard_id=dashboard_id,
        title=title,
        description=description,
        owners=[DEFAULT_OWNER_USER_ID],
        published=True,
        json_metadata=definition.get("metadata"),
    )

    superset_id = dashboard.get("id", dashboard_id)

    # Build position JSON based on chart definitions
    position = _build_dashboard_position(definition.get("charts", []))
    client.update_dashboard(
        int(superset_id),
        position_json=json.dumps(position),
        json_metadata=json.dumps(definition.get("metadata", {})),
    )

    logger.info(
        "Dashboard scaffold created | id=%d | title='%s' | charts=%d",
        superset_id, title, len(definition.get("charts", []))
    )

    return {
        "dashboard_id": dashboard_id,
        "title": title,
        "status": "CREATED",
        "superset_id": superset_id,
        "charts_count": len(definition.get("charts", [])),
    }


def _build_dashboard_position(
    charts: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Build Superset position JSON from chart definitions.

    Superset uses a nested grid system where each element has
    type, id, children, and meta (size/position).
    """
    root_children: list[dict[str, Any]] = []
    chart_index = 0

    # Group charts by row from their position metadata
    rows_dict: dict[int, list[dict[str, Any]]] = {}
    for chart in charts:
        pos = chart.get("position", {})
        row_num = pos.get("row", 1)
        if row_num not in rows_dict:
            rows_dict[row_num] = []
        rows_dict[row_num].append(chart)

    for row_num in sorted(rows_dict.keys()):
        row_charts = rows_dict[row_num]
        row_children: list[dict[str, Any]] = []

        for chart in sorted(row_charts, key=lambda c: c.get("position", {}).get("col", 1)):
            pos = chart.get("position", {})
            chart_index += 1
            chart_id = f"CHART-{chart_index}"
            row_children.append({
                "type": "CHART",
                "id": chart_id,
                "meta": {
                    "width": pos.get("size_x", 6),
                    "height": pos.get("size_y", 3) * 10,  # Grid units
                    "chartId": chart_index,
                },
            })

        root_children.append({
            "type": "ROW",
            "id": f"ROW-{row_num}",
            "children": row_children,
        })

    return {
        "root": "ROOT",
        "children": root_children,
        "chartId": 0,
    }


def seed_all_dashboards(
    client: SupersetClient,
    dry_run: bool = False,
    dashboard_ids: Optional[list[int]] = None,
) -> dict[str, Any]:
    """
    Seed all (or specific) dashboard scaffolds.

    Args:
        client: SupersetClient instance.
        dry_run: If True, only show what would be created.
        dashboard_ids: If provided, only create these specific dashboard IDs.

    Returns:
        Dict with results.
    """
    definitions = DASHBOARD_DEFINITIONS
    if dashboard_ids:
        definitions = [d for d in definitions if d["id"] in dashboard_ids]

    results: list[dict[str, Any]] = []
    for definition in definitions:
        result = create_dashboard_scaffold(client, definition, dry_run=dry_run)
        results.append(result)

    created = sum(1 for r in results if r["status"] in ("CREATED", "EXISTS"))
    skipped = sum(1 for r in results if r["status"] == "DRY-RUN")

    return {
        "total_definitions": len(definitions),
        "created_or_exists": created,
        "dry_run_skipped": skipped,
        "results": results,
    }


# ============================================================================
# CLI Entry Point
# ============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Seed dashboard scaffolds in Superset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--create-all",
        action="store_true",
        help="Create all 5 dashboard scaffolds",
    )
    parser.add_argument(
        "--dashboard",
        type=int,
        metavar="ID",
        help="Create a specific dashboard by ID (1-5)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be created without making changes",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List current dashboards in Superset",
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
    logger.info("Superset Dashboard Seeder — Phase 13")
    logger.info("=" * 60)

    client = SupersetClient(
        base_url=SUPERSET_URL,
        username=SUPERSET_ADMIN_USER,
        password=SUPERSET_ADMIN_PWD,
    )

    if args.list:
        list_dashboards_in_superset(client)
        sys.exit(0)

    if not any([args.create_all, args.dashboard]):
        print("Available dashboards to create:")
        for d in DASHBOARD_DEFINITIONS:
            print(f"  [{d['id']}] {d['title']}")
        print("\nUse --create-all or --dashboard <id>")
        sys.exit(0)

    if args.dashboard:
        dashboard_ids = [args.dashboard]
    else:
        dashboard_ids = None

    result = seed_all_dashboards(
        client=client,
        dry_run=args.dry_run,
        dashboard_ids=dashboard_ids,
    )

    print("\n" + "=" * 60)
    print("SEED RESULT")
    print("=" * 60)
    print(f"  Total definitions : {result['total_definitions']}")
    print(f"  Created/Exists  : {result['created_or_exists']}")
    print(f"  Dry-run skipped  : {result['dry_run_skipped']}")
    for r in result["results"]:
        status_icon = {"CREATED": "✓", "EXISTS": "=", "DRY-RUN": "~"}.get(
            r["status"], r["status"]
        )
        charts_info = f" (charts: {r.get('charts_count', 0)})" if r.get('charts_count') else ""
        print(f"  {status_icon} [{r['dashboard_id']}] {r['title']}{charts_info} [{r['status']}]")
    print("=" * 60)
    sys.exit(0)


if __name__ == "__main__":
    main()