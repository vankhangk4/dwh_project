"""
api/routes/etl.py
Phase 12: ETL management routes for Auth Gateway.

Provides:
    - POST /etl/trigger       : Trigger ETL pipeline for specified tenants (admin only).
    - GET  /etl/status        : Get ETL scheduler status and recent run history.
    - POST /etl/trigger/sync  : Trigger ETL synchronously (wait for completion).

Author: Nguyen Van Khang
"""

from __future__ import annotations

import logging
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Annotated, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import text
from sqlalchemy.engine import Connection

from api.config import get_config
from api.database import get_db
from api.middleware import AdminUser, AuthenticatedUser
from api.models import (
    ETLStatusResponse,
    ETLTriggerRequest,
    ETLTriggerResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/etl", tags=["ETL"])

# Thread pool for background ETL execution
_etl_executor = ThreadPoolExecutor(max_workers=4)

# Track background job status (in-memory; use Redis in production)
_background_jobs: dict[str, dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# Trigger ETL
# ---------------------------------------------------------------------------

@router.post(
    "/trigger",
    response_model=ETLTriggerResponse,
    summary="Trigger ETL pipeline",
    description="""
    Trigger the ETL pipeline for specified tenants (or all active tenants if none specified).

    **Requires:** Admin role.

    The pipeline runs Extract → Transform → Load → Execute SPs → Refresh Data Marts.

    By default this returns immediately with a background job ID.
    Use `/etl/status` to check the job status.
    """,
    responses={
        200: {"description": "ETL triggered successfully"},
        202: {"description": "ETL accepted and queued"},
        401: {"description": "Authentication required"},
        403: {"description": "Admin role required"},
    },
)
async def trigger_etl(
    request: ETLTriggerRequest,
    db: Annotated[Connection, Depends(get_db)],
    current_user: AdminUser,
) -> ETLTriggerResponse:
    """
    Trigger ETL pipeline for tenants.

    **Requires:** Admin role.

    Runs the ETL pipeline in the background (thread pool) to avoid blocking
    the HTTP response. Returns a job_id that can be used to check status.

    If tenant_ids is empty/None, runs for all active tenants.

    Args:
        request: ETL trigger request with tenant_ids, stage_only, skip_sources.
        db: Database connection (not used in this endpoint, for injection only).
        current_user: Authenticated admin user.

    Returns:
        ETLTriggerResponse with status, job_id, and message.
    """
    logger.info(
        "trigger_etl | user_id=%d | tenant_ids=%s | stage_only=%s",
        current_user.user_id,
        request.tenant_ids or "ALL",
        request.stage_only,
    )

    job_id = str(uuid.uuid4())[:16]

    _background_jobs[job_id] = {
        "status": "PENDING",
        "tenant_ids": request.tenant_ids,
        "stage_only": request.stage_only,
        "skip_sources": request.skip_sources,
        "submitted_by": current_user.user_id,
        "submitted_at": datetime.now().isoformat(),
        "started_at": None,
        "completed_at": None,
        "result": None,
        "error": None,
    }

    # Submit background ETL job
    try:
        _etl_executor.submit(
            _run_etl_background,
            job_id,
            request.tenant_ids,
            request.stage_only,
            request.skip_sources,
            current_user.user_id,
        )

        logger.info(
            "ETL job submitted | job_id=%s | tenant_ids=%s",
            job_id, request.tenant_ids or "ALL"
        )

        return ETLTriggerResponse(
            status="PENDING",
            message=(
                f"ETL pipeline submitted successfully. "
                f"Job ID: {job_id}. Use GET /etl/status to check progress."
            ),
            job_id=job_id,
        )

    except Exception as ex:
        logger.error("Failed to submit ETL job: %s", ex)
        _background_jobs[job_id]["status"] = "FAILED"
        _background_jobs[job_id]["error"] = str(ex)

        return ETLTriggerResponse(
            status="FAILED",
            message=f"Failed to submit ETL job: {ex}",
            job_id=job_id,
        )


def _run_etl_background(
    job_id: str,
    tenant_ids: Optional[list[str]],
    stage_only: bool,
    skip_sources: Optional[list[str]],
    user_id: int,
) -> None:
    """
    Background worker that runs the ETL pipeline.

    This function runs in a separate thread to avoid blocking the HTTP response.
    Updates _background_jobs status as it progresses.
    """
    try:
        logger.info("ETL background job started | job_id=%s", job_id)
        _background_jobs[job_id]["status"] = "RUNNING"
        _background_jobs[job_id]["started_at"] = datetime.now().isoformat()

        # Import here to avoid circular imports
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

        from etl.orchestrator.orchestrator import run_all_tenants
        from etl.utils.logging_config import setup_logging

        setup_logging(log_level="INFO", log_file=f"etl_{job_id}.log")

        result = run_all_tenants(
            tenant_ids=tenant_ids,
            stage_only=stage_only,
            send_alerts=False,
            skip_sources=skip_sources,
            truncate_before_load=True,
        )

        logger.info(
            "ETL background job completed | job_id=%s | status=%s",
            job_id, result["overall_status"]
        )

        _background_jobs[job_id]["status"] = result["overall_status"]
        _background_jobs[job_id]["result"] = result
        _background_jobs[job_id]["completed_at"] = datetime.now().isoformat()

    except Exception as ex:
        logger.error("ETL background job failed | job_id=%s | error=%s", job_id, ex)
        _background_jobs[job_id]["status"] = "FAILED"
        _background_jobs[job_id]["error"] = str(ex)
        _background_jobs[job_id]["completed_at"] = datetime.now().isoformat()


# ---------------------------------------------------------------------------
# ETL Status
# ---------------------------------------------------------------------------

@router.get(
    "/status",
    response_model=ETLStatusResponse,
    summary="Get ETL status",
    description="""
    Get the current ETL scheduler status and recent run history.

    Returns:
        - Scheduler running status.
        - Next scheduled run time.
        - Recent ETL run history (from ETL_RunLog table).
    """,
    responses={
        200: {"description": "ETL status retrieved"},
        401: {"description": "Authentication required"},
    },
)
async def get_etl_status(
    db: Annotated[Connection, Depends(get_db)],
    current_user: AuthenticatedUser,
    job_id: Optional[str] = None,
) -> ETLStatusResponse:
    """
    Get ETL status and scheduler state.

    Args:
        db: Database connection.
        current_user: Authenticated user.
        job_id: Optional specific job ID to get details for.

    Returns:
        ETLStatusResponse with scheduler status, next run, and recent runs.
    """
    logger.info(
        "get_etl_status | user_id=%d | job_id=%s",
        current_user.user_id, job_id or "N/A"
    )

    # If specific job_id requested, return that job's status
    if job_id and job_id in _background_jobs:
        job = _background_jobs[job_id]
        return ETLStatusResponse(
            status=job["status"],
            scheduler_running=True,
            next_run=None,
            recent_runs=[
                {
                    "job_id": job_id,
                    "status": job["status"],
                    "submitted_at": job["submitted_at"],
                    "started_at": job.get("started_at"),
                    "completed_at": job.get("completed_at"),
                    "tenant_ids": job.get("tenant_ids"),
                    "stage_only": job.get("stage_only"),
                    "error": job.get("error"),
                    "result_summary": _summarize_result(job.get("result")),
                }
            ],
        )

    # Get scheduler status from APScheduler
    scheduler_running = False
    next_run = None

    try:
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        from etl.orchestrator.scheduler import get_scheduler_status, is_scheduler_running

        scheduler_running = is_scheduler_running()
        if scheduler_running:
            sched_status = get_scheduler_status()
            jobs = sched_status.get("jobs", [])
            if jobs:
                next_run = jobs[0].get("next_run")

    except Exception as ex:
        logger.debug("Scheduler status unavailable: %s", ex)

    # Get recent ETL runs from database
    recent_runs: list[dict[str, Any]] = []

    try:
        query = text("""
            SELECT TOP 20
                TenantID,
                StoredProcedureName,
                Status,
                RowsProcessed,
                StartTime,
                EndTime,
                DurationSeconds,
                ErrorMessage
            FROM ETL_RunLog
            ORDER BY StartTime DESC
        """)

        result = db.execute(query)
        db.commit()
        rows = result.fetchall()

        for row in rows:
            recent_runs.append({
                "tenant_id": row[0],
                "procedure": row[1],
                "status": row[2],
                "rows_processed": row[3],
                "start_time": row[4].isoformat() if row[4] else None,
                "end_time": row[5].isoformat() if row[5] else None,
                "duration_seconds": row[6],
                "error": row[7],
            })

    except Exception as ex:
        logger.warning("Failed to fetch ETL run history: %s", ex)

    # Include pending/running background jobs
    for jid, job in _background_jobs.items():
        if job["status"] in ("PENDING", "RUNNING"):
            recent_runs.insert(0, {
                "job_id": jid,
                "status": job["status"],
                "submitted_at": job["submitted_at"],
                "started_at": job.get("started_at"),
                "completed_at": job.get("completed_at"),
                "tenant_ids": job.get("tenant_ids"),
                "stage_only": job.get("stage_only"),
            })

    return ETLStatusResponse(
        status="RUNNING" if scheduler_running else "STOPPED",
        scheduler_running=scheduler_running,
        next_run=next_run,
        recent_runs=recent_runs,
    )


def _summarize_result(result: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    """Create a compact summary of ETL result for display."""
    if result is None:
        return None
    return {
        "overall_status": result.get("overall_status"),
        "total_tenants": result.get("total_tenants"),
        "tenants_succeeded": result.get("tenants_succeeded"),
        "tenants_failed": result.get("tenants_failed"),
        "total_rows_loaded": result.get("total_rows_loaded"),
        "duration_seconds": result.get("duration_seconds"),
    }


# ---------------------------------------------------------------------------
# Sync trigger (wait for completion)
# ---------------------------------------------------------------------------

@router.post(
    "/trigger/sync",
    response_model=ETLTriggerResponse,
    summary="Trigger ETL synchronously",
    description="""
    Trigger ETL and wait for completion.

    **Requires:** Admin role.

    This endpoint blocks until the ETL pipeline completes.
    Use for quick testing or when you need the result immediately.

    Warning: This can take several minutes for large datasets.
    Prefer the async `/etl/trigger` endpoint for production use.
    """,
    responses={
        200: {"description": "ETL completed successfully"},
        401: {"description": "Authentication required"},
        403: {"description": "Admin role required"},
        504: {"description": "ETL timed out"},
    },
)
async def trigger_etl_sync(
    request: ETLTriggerRequest,
    db: Annotated[Connection, Depends(get_db)],
    current_user: AdminUser,
    timeout_seconds: int = 600,
) -> ETLTriggerResponse:
    """
    Trigger ETL synchronously (blocking).

    **Requires:** Admin role.

    Runs ETL and waits for completion. Returns full result.

    Args:
        request: ETL trigger request.
        db: Database connection.
        current_user: Authenticated admin user.
        timeout_seconds: Maximum time to wait. (default 600 = 10 minutes)

    Returns:
        ETLTriggerResponse with full ETL results.
    """
    logger.info(
        "trigger_etl_sync | user_id=%d | tenant_ids=%s | stage_only=%s | timeout=%ds",
        current_user.user_id,
        request.tenant_ids or "ALL",
        request.stage_only,
        timeout_seconds,
    )

    try:
        import sys
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

        from etl.orchestrator.orchestrator import run_all_tenants
        from etl.utils.logging_config import setup_logging

        setup_logging(log_level="INFO")

        result = run_all_tenants(
            tenant_ids=request.tenant_ids,
            stage_only=request.stage_only,
            send_alerts=False,
            skip_sources=request.skip_sources,
            truncate_before_load=True,
        )

        status_str = result.get("overall_status", "UNKNOWN")
        rows_loaded = result.get("total_rows_loaded", 0)
        duration = result.get("duration_seconds", 0)

        logger.info(
            "trigger_etl_sync completed | status=%s | rows=%d | duration=%ds",
            status_str, rows_loaded, duration
        )

        return ETLTriggerResponse(
            status=status_str,
            message=f"ETL completed: {status_str} — {rows_loaded} rows loaded in {duration}s",
            results=result,
        )

    except Exception as ex:
        logger.error("trigger_etl_sync failed: %s", ex, exc_info=True)
        return ETLTriggerResponse(
            status="FAILED",
            message=f"ETL pipeline failed: {ex}",
        )