"""
etl/orchestrator/orchestrator.py
Phase 11: Orchestrator — runs ETL pipeline for all active tenants.

Reads tenant list from Tenants table (SQL Server), then runs the ETL pipeline
sequentially for each active tenant.

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime
from typing import Any, Optional

import pandas as pd

from etl.extract.db_utils import (
    get_db_connection,
    close_connection,
    execute_sql,
)
from etl.orchestrator.config import get_config, ETLConfig
from etl.orchestrator.etl_pipeline import run_etl_for_tenant
from etl.utils.logging_config import setup_logging, get_logger
from etl.utils.monitoring import (
    alert_pipeline_success,
    alert_pipeline_failure,
    alert_pipeline_warning,
    alert,
    AlertLevel,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Main orchestrator function
# ---------------------------------------------------------------------------

def run_all_tenants(
    tenant_ids: Optional[list[str]] = None,
    conn_str: Optional[str] = None,
    stage_only: bool = False,
    send_alerts: bool = True,
    skip_sources: Optional[list[str]] = None,
    run_transform: bool = True,
    run_staging_load: bool = True,
    run_sp_load: bool = True,
    run_datamart: bool = True,
    truncate_before_load: bool = True,
    log: Optional[logging.Logger] = None,
) -> dict[str, Any]:
    """
    Run ETL pipeline for all active tenants (or a specified subset).

    Orchestrates the full ETL process:
        1. Setup logging
        2. Load configuration
        3. Get active tenant list (from DB or parameter)
        4. For each tenant:
            a. Resolve file paths from config
            b. Run run_etl_for_tenant()
            c. Collect results
        5. Aggregate results
        6. Send summary alert

    Args:
        tenant_ids:    List of tenant IDs to process.
                     If None, reads from Tenants table in DB. (optional)
        conn_str:     SQL Server connection string.
                     If None, reads from CONN_STR env var. (optional)
        stage_only:   Only run Extract + Transform + Load to staging.
                     Skips SP execution and datamart refresh. (default False)
        send_alerts: Send email/Slack alerts. (default True)
        skip_sources: List of source types to skip. (optional)
        run_transform: Run transform step. (default True)
        run_staging_load: Load to staging tables. (default True)
        run_sp_load:  Execute stored procedures. (default True)
        run_datamart: Refresh data marts. (default True)
        truncate_before_load: Truncate staging before load. (default True)
        log:         Scoped logger. If None, uses root etl logger. (optional)

    Returns:
        Dict with aggregated results:
            {
                "overall_status": "SUCCESS" | "WARNING" | "FAILED",
                "start_time": datetime,
                "end_time": datetime,
                "duration_seconds": int,
                "total_tenants": int,
                "tenants_succeeded": int,
                "tenants_failed": int,
                "total_rows_processed": int,
                "results": {
                    "STORE_HN": {...},
                    "STORE_HCM": {...},
                },
                "errors": [...],
            }
    """
    if log is None:
        log = logger

    config = get_config()

    overall_start = datetime.now()
    overall_result: dict[str, Any] = {
        "overall_status": "SUCCESS",
        "start_time": overall_start.isoformat(),
        "end_time": None,
        "duration_seconds": 0,
        "total_tenants": 0,
        "tenants_succeeded": 0,
        "tenants_failed": 0,
        "tenants_skipped": 0,
        "total_rows_processed": 0,
        "total_rows_loaded": 0,
        "results": {},
        "errors": [],
        "warnings": [],
    }

    log.info("=" * 70)
    log.info("ETL ORCHESTRATOR: Starting run for all tenants")
    log.info("=" * 70)

    if stage_only:
        log.info("Mode: STAGE ONLY (skip SP execution and datamart refresh)")

    # ---- Step 1: Resolve tenant IDs ----
    if tenant_ids is None:
        log.info("No tenant_ids provided. Reading from Tenants table...")
        try:
            tenant_ids = _get_active_tenants(conn_str=conn_str)
            log.info("Found %d active tenants: %s", len(tenant_ids), tenant_ids)
        except Exception as ex:
            log.error("Failed to read tenant list: %s", ex)
            overall_result["overall_status"] = "FAILED"
            overall_result["errors"].append(f"Failed to read tenants: {ex}")
            return overall_result
    else:
        log.info("Processing %d specified tenants: %s", len(tenant_ids), tenant_ids)

    if not tenant_ids:
        log.warning("No tenants to process. Exiting.")
        overall_result["warnings"].append("No tenants found or specified")
        overall_result["end_time"] = datetime.now().isoformat()
        return overall_result

    overall_result["total_tenants"] = len(tenant_ids)

    # ---- Step 2: Run ETL for each tenant ----
    for idx, tenant_id in enumerate(tenant_ids, start=1):
        tenant_log = get_logger(
            "etl.orchestrator",
            tenant_id=tenant_id,
            pipeline_name=f"tenant_run",
        )

        tenant_start = time.time()
        log.info(
            "--- [%d/%d] Processing tenant: %s ---",
            idx, len(tenant_ids), tenant_id
        )

        try:
            file_paths = _resolve_file_paths(config, tenant_id)
            tenant_log.info("Resolved file paths: %s", _summarize_paths(file_paths))

            tenant_result = run_etl_for_tenant(
                tenant_id=tenant_id,
                file_paths=file_paths,
                conn_str=conn_str,
                send_alerts=send_alerts,
                stage_only=stage_only,
                skip_sources=skip_sources,
                run_transform=run_transform,
                run_staging_load=run_staging_load,
                run_sp_load=run_sp_load,
                run_datamart=run_datamart,
                truncate_before_load=truncate_before_load,
            )

            overall_result["results"][tenant_id] = tenant_result
            overall_result["total_rows_processed"] += tenant_result.get(
                "total_rows_processed", 0)
            overall_result["total_rows_loaded"] += tenant_result.get(
                "total_rows_loaded", 0)

            tenant_duration = int(time.time() - tenant_start)
            if tenant_result["status"] == "SUCCESS":
                overall_result["tenants_succeeded"] += 1
                log.info(
                    "--- [%d/%d] Tenant %s: SUCCESS (%ds) ---",
                    idx, len(tenant_ids), tenant_id, tenant_duration
                )
            elif tenant_result["status"] == "WARNING":
                overall_result["tenants_failed"] += 1
                log.warning(
                    "--- [%d/%d] Tenant %s: WARNING (%ds) ---",
                    idx, len(tenant_ids), tenant_id, tenant_duration
                )
            else:
                overall_result["tenants_failed"] += 1
                log.error(
                    "--- [%d/%d] Tenant %s: FAILED (%ds) ---",
                    idx, len(tenant_ids), tenant_id, tenant_duration
                )
                if tenant_result.get("errors"):
                    for err in tenant_result["errors"]:
                        overall_result["errors"].append(f"[{tenant_id}] {err}")

        except Exception as ex:
            overall_result["tenants_failed"] += 1
            overall_result["errors"].append(f"[{tenant_id}] {ex}")
            log.error(
                "--- [%d/%d] Tenant %s: CATASTROPHIC FAILURE ---",
                idx, len(tenant_ids), tenant_id
            )
            log.error("Error: %s", ex, exc_info=True)

            if send_alerts:
                try:
                    alert_pipeline_failure(
                        tenant_id=tenant_id,
                        pipeline_name="orchestrator",
                        error_detail=str(ex),
                    )
                except Exception:
                    pass

    # ---- Step 3: Determine overall status ----
    overall_end = datetime.now()
    overall_duration = int((overall_end - overall_start).total_seconds())
    overall_result["end_time"] = overall_end.isoformat()
    overall_result["duration_seconds"] = overall_duration

    if overall_result["tenants_failed"] == 0:
        overall_result["overall_status"] = "SUCCESS"
    elif overall_result["tenants_succeeded"] == 0:
        overall_result["overall_status"] = "FAILED"
    else:
        overall_result["overall_status"] = "WARNING"

    # ---- Step 4: Summary log ----
    log.info("=" * 70)
    log.info("ETL ORCHESTRATOR: COMPLETED")
    log.info("  Overall status: %s", overall_result["overall_status"])
    log.info("  Tenants: %d total | %d succeeded | %d failed | %d skipped",
             overall_result["total_tenants"],
             overall_result["tenants_succeeded"],
             overall_result["tenants_failed"],
             overall_result["tenants_skipped"])
    log.info("  Rows processed: %d", overall_result["total_rows_processed"])
    log.info("  Rows loaded: %d", overall_result["total_rows_loaded"])
    log.info("  Total duration: %ds", overall_duration)
    log.info("=" * 70)

    # ---- Step 5: Send summary alert ----
    if send_alerts and overall_result["total_tenants"] > 1:
        try:
            _send_summary_alert(overall_result)
        except Exception as ex:
            log.warning("Summary alert failed: %s", ex)

    return overall_result


# ---------------------------------------------------------------------------
# Tenant management
# ---------------------------------------------------------------------------

def _get_active_tenants(conn_str: Optional[str] = None) -> list[str]:
    """
    Read active tenant IDs from the Tenants table in SQL Server.

    Args:
        conn_str: SQL Server connection string. (optional, reads from env)

    Returns:
        List of active tenant IDs.
    """
    if conn_str is None:
        conn_str = os.environ.get("CONN_STR")

    conn = get_db_connection()
    try:
        df = execute_sql(conn, """
            SELECT TenantID
            FROM Tenants
            WHERE IsActive = 1
            ORDER BY TenantID
        """, fetch=True)

        if df is None or df.empty:
            return []

        return df["TenantID"].tolist()
    finally:
        close_connection(conn)


# ---------------------------------------------------------------------------
# File path resolution
# ---------------------------------------------------------------------------

def _resolve_file_paths(config: ETLConfig, tenant_id: str) -> dict[str, str]:
    """
    Resolve file paths for a tenant from the configuration.

    Returns:
        Dict of source type -> absolute file path.
    """
    fp = config.files
    paths: dict[str, str] = {}

    candidates = {
        "sales": fp.get_sales_path(tenant_id),
        "inventory": fp.get_inventory_path(tenant_id),
        "purchase": fp.get_purchase_path(tenant_id),
        "customer": fp.get_customer_path(tenant_id),
        "employee": fp.get_employee_path(tenant_id),
        "store": fp.get_store_path(tenant_id),
    }

    for key, path in candidates.items():
        if path:
            paths[key] = path

    paths["product"] = fp.get_product_path()
    paths["supplier"] = fp.get_supplier_path()

    return paths


# ---------------------------------------------------------------------------
# Summary alert
# ---------------------------------------------------------------------------

def _send_summary_alert(result: dict[str, Any]) -> None:
    """
    Send a summary alert after all tenants have been processed.
    """
    total = result["total_tenants"]
    succeeded = result["tenants_succeeded"]
    failed = result["tenants_failed"]
    duration = result["duration_seconds"]

    status_emoji = {
        "SUCCESS": "✅",
        "WARNING": "⚠️",
        "FAILED": "❌",
    }
    emoji = status_emoji.get(result["overall_status"], "ℹ️")

    message = (
        f"{emoji} ETL Orchestrator completed: "
        f"{succeeded}/{total} tenants succeeded, "
        f"{failed} failed, in {duration}s"
    )

    level = (
        AlertLevel.ERROR if result["overall_status"] == "FAILED"
        else AlertLevel.WARNING if result["overall_status"] == "WARNING"
        else AlertLevel.INFO
    )

    context = {
        "total_tenants": str(total),
        "succeeded": str(succeeded),
        "failed": str(failed),
        "rows_loaded": str(result["total_rows_loaded"]),
        "duration": f"{duration}s",
    }

    alert(
        message=message,
        level=level,
        pipeline_name="orchestrator",
        duration_seconds=duration,
        context=context,
        send_email=True,
        send_slack=True,
    )


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _summarize_paths(paths: dict[str, str]) -> str:
    """Compact summary of file paths."""
    if not paths:
        return "(none)"
    import os
    return ", ".join(
        f"{k}={os.path.basename(v)}"
        for k, v in paths.items()
    )
