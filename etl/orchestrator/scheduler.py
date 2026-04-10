"""
etl/orchestrator/scheduler.py
Phase 11: APScheduler-based ETL job scheduler.

Provides:
    - start_scheduler()       : Start the APScheduler background scheduler
    - stop_scheduler()        : Stop the scheduler
    - schedule_daily_etl()   : Schedule daily ETL run at 02:00 AM
    - schedule_weekly_etl() : Schedule weekly ETL run (configurable day/time)
    - run_now()             : Trigger immediate ETL run (manual trigger)
    - get_scheduler_status() : Get current scheduler state

Usage:
    scheduler = start_scheduler()
    schedule_daily_etl(scheduler, hour=2, minute=0)
    scheduler.start()
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime
from typing import Any, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from etl.orchestrator.config import get_config
from etl.orchestrator.orchestrator import run_all_tenants
from etl.utils.logging_config import get_logger

logger = logging.getLogger(__name__)

# Global scheduler instance
_scheduler: Optional[BackgroundScheduler] = None
_scheduler_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

def start_scheduler() -> BackgroundScheduler:
    """
    Start the APScheduler background scheduler.

    Returns:
        BackgroundScheduler instance (singleton).
    """
    global _scheduler

    with _scheduler_lock:
        if _scheduler is not None and _scheduler.running:
            logger.info("Scheduler already running. Returning existing instance.")
            return _scheduler

        _scheduler = BackgroundScheduler(
            daemon=True,
            job_defaults={
                "coalesce": True,
                "max_instances": 1,
                "misfire_grace_time": 3600,
            },
        )
        _scheduler.start()
        logger.info("ETL scheduler started")
        return _scheduler


def stop_scheduler() -> None:
    """
    Stop the APScheduler scheduler gracefully.

    Shuts down all pending jobs before stopping.
    """
    global _scheduler

    with _scheduler_lock:
        if _scheduler is None:
            logger.info("Scheduler not running. Nothing to stop.")
            return

        if _scheduler.running:
            logger.info("Stopping ETL scheduler...")
            _scheduler.shutdown(wait=True)
            logger.info("ETL scheduler stopped.")
        _scheduler = None


def is_scheduler_running() -> bool:
    """
    Check if the scheduler is currently running.

    Returns:
        True if running, False otherwise.
    """
    return _scheduler is not None and _scheduler.running


def get_scheduler_status() -> dict[str, Any]:
    """
    Get the current scheduler state.

    Returns:
        Dict with scheduler status, jobs, and next run times.
    """
    global _scheduler

    if _scheduler is None:
        return {
            "running": False,
            "jobs": [],
            "next_run": None,
        }

    jobs = []
    for job in (_scheduler.get_jobs() or []):
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger),
        })

    return {
        "running": _scheduler.running,
        "jobs": jobs,
        "next_run": (
            _scheduler.get_jobs()[0].next_run_time.isoformat()
            if _scheduler.get_jobs() else None
        ),
    }


# ---------------------------------------------------------------------------
# Job registration
# ---------------------------------------------------------------------------

def schedule_daily_etl(
    hour: int = 2,
    minute: int = 0,
    job_id: str = "daily_etl",
    job_name: str = "Daily ETL Pipeline",
    kwargs: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    """
    Schedule daily ETL run at a specific hour and minute.

    Default: 02:00 AM every day.

    Args:
        hour:    Hour of day (0-23). (default 2)
        minute:  Minute (0-59). (default 0)
        job_id:  Unique job identifier. (default "daily_etl")
        job_name: Display name for the job. (default "Daily ETL Pipeline")
        kwargs:  Additional keyword args passed to run_all_tenants(). (optional)

    Returns:
        Job ID if scheduled successfully, None if scheduler not running.

    Example:
        schedule_daily_etl(hour=2, minute=30)
        # Runs every day at 02:30 AM
    """
    global _scheduler

    if not is_scheduler_running():
        logger.warning("Scheduler not running. Cannot schedule daily ETL.")
        return None

    if kwargs is None:
        kwargs = {}

    job = _scheduler.add_job(
        func=_etl_job_wrapper,
        trigger=CronTrigger(hour=hour, minute=minute),
        id=job_id,
        name=job_name,
        kwargs=kwargs,
        replace_existing=True,
    )

    logger.info(
        "Daily ETL job scheduled | id=%s | time=%02d:%02d | next_run=%s",
        job_id, hour, minute,
        job.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if job.next_run_time else "N/A"
    )

    return job_id


def schedule_weekly_etl(
    day_of_week: str = "sunday",
    hour: int = 2,
    minute: int = 0,
    job_id: str = "weekly_etl",
    job_name: str = "Weekly ETL Pipeline",
    kwargs: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    """
    Schedule weekly ETL run on a specific day of the week.

    Args:
        day_of_week: Day name (monday, tuesday, ..., sunday). (default "sunday")
        hour:        Hour (0-23). (default 2)
        minute:      Minute (0-59). (default 0)
        job_id:      Unique job identifier. (default "weekly_etl")
        job_name:    Display name. (default "Weekly ETL Pipeline")
        kwargs:       Additional args for run_all_tenants(). (optional)

    Returns:
        Job ID if scheduled, None if scheduler not running.
    """
    global _scheduler

    if not is_scheduler_running():
        logger.warning("Scheduler not running. Cannot schedule weekly ETL.")
        return None

    if kwargs is None:
        kwargs = {}

    day_map = {
        "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
        "friday": 4, "saturday": 5, "sunday": 6,
    }
    day_num = day_map.get(day_of_week.lower(), 6)

    job = _scheduler.add_job(
        func=_etl_job_wrapper,
        trigger=CronTrigger(day_of_week=day_num, hour=hour, minute=minute),
        id=job_id,
        name=job_name,
        kwargs=kwargs,
        replace_existing=True,
    )

    logger.info(
        "Weekly ETL job scheduled | id=%s | day=%s | time=%02d:%02d | next_run=%s",
        job_id, day_of_week, hour, minute,
        job.next_run_time.strftime("%Y-%m-%d %H:%M:%S") if job.next_run_time else "N/A"
    )

    return job_id


def run_now(
    tenant_ids: Optional[list[str]] = None,
    stage_only: bool = False,
    send_alerts: bool = True,
    **kwargs,
) -> dict[str, Any]:
    """
    Trigger an immediate ETL run (manual trigger, bypassing the scheduler).

    This is a synchronous call — it blocks until the ETL completes.

    Args:
        tenant_ids:   List of tenant IDs to process. (optional)
        stage_only:   Only run Extract + Transform + Load to staging. (default False)
        send_alerts:  Send alerts. (default True)
        **kwargs:     Additional args for run_all_tenants().

    Returns:
        Dict with ETL results from run_all_tenants().
    """
    log = get_logger("etl.scheduler", pipeline_name="manual_trigger")

    log.info("Manual ETL trigger started | tenant_ids=%s | stage_only=%s",
             tenant_ids or "ALL", stage_only)

    try:
        result = run_all_tenants(
            tenant_ids=tenant_ids,
            stage_only=stage_only,
            send_alerts=send_alerts,
            log=log,
            **kwargs,
        )
        log.info("Manual ETL trigger completed | status=%s | duration=%ds",
                 result["overall_status"], result["duration_seconds"])
        return result

    except Exception as ex:
        log.error("Manual ETL trigger FAILED: %s", ex, exc_info=True)
        return {
            "overall_status": "FAILED",
            "error": str(ex),
            "start_time": datetime.now().isoformat(),
            "end_time": datetime.now().isoformat(),
        }


def remove_job(job_id: str) -> bool:
    """
    Remove a scheduled job from the scheduler.

    Args:
        job_id: Job identifier to remove.

    Returns:
        True if removed, False if not found or scheduler not running.
    """
    global _scheduler

    if not is_scheduler_running():
        return False

    try:
        _scheduler.remove_job(job_id)
        logger.info("Job '%s' removed from scheduler", job_id)
        return True
    except Exception:
        logger.warning("Job '%s' not found in scheduler", job_id)
        return False


# ---------------------------------------------------------------------------
# Job wrapper
# ---------------------------------------------------------------------------

def _etl_job_wrapper(**kwargs) -> dict[str, Any]:
    """
    Internal wrapper for scheduled ETL jobs.

    Handles logging and error propagation for APScheduler.
    """
    log = get_logger("etl.scheduler", pipeline_name="scheduled_etl")

    log.info(
        "Scheduled ETL job starting | time=%s",
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )

    try:
        result = run_all_tenants(
            send_alerts=True,
            log=log,
            **kwargs,
        )
        log.info(
            "Scheduled ETL job completed | status=%s | duration=%ds",
            result["overall_status"], result["duration_seconds"]
        )
        return result

    except Exception as ex:
        log.error("Scheduled ETL job FAILED: %s", ex, exc_info=True)
        return {
            "overall_status": "FAILED",
            "error": str(ex),
        }