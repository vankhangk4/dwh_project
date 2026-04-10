"""
etl/orchestrator/__main__.py
Phase 11: Entry point for ETL orchestrator.

Usage:
    # Run ETL for all active tenants
    python -m etl.orchestrator

    # Run ETL for specific tenants only
    python -m etl.orchestrator --tenants STORE_HN --tenants STORE_HCM

    # Stage-only mode (Extract + Transform + Load to staging, skip SP and DM)
    python -m etl.orchestrator --stage-only

    # Dry run (no alerts)
    python -m etl.orchestrator --no-alerts

    # Skip specific sources
    python -m etl.orchestrator --skip sales --skip inventory

    # Start scheduler mode
    python -m etl.orchestrator --scheduler --schedule "0 2 * * *"

    # Run scheduler with daily ETL at 02:30 AM
    python -m etl.orchestrator --scheduler --daily 2 --minute 30

Author: Nguyen Van Khang
"""

from __future__ import annotations

import argparse
import logging
import json
import os
import sys
import signal
from datetime import datetime

from etl.orchestrator.config import get_config
from etl.orchestrator.orchestrator import run_all_tenants
from etl.orchestrator.scheduler import (
    start_scheduler,
    stop_scheduler,
    schedule_daily_etl,
    schedule_weekly_etl,
    is_scheduler_running,
    get_scheduler_status,
)
from etl.utils.logging_config import setup_logging

logger: logging.Logger | None = None


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m etl.orchestrator",
        description="ETL Orchestrator — Data Warehouse Multi-Tenant Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m etl.orchestrator

  python -m etl.orchestrator --tenants STORE_HN

  python -m etl.orchestrator --tenants STORE_HN --tenants STORE_HCM

  python -m etl.orchestrator --stage-only

  python -m etl.orchestrator --scheduler --daily 2 --minute 30

  python -m etl.orchestrator --scheduler --weekly sunday --hour 3

  python -m etl.orchestrator --status
        """,
    )

    parser.add_argument(
        "--tenants", action="append",
        help="Tenant IDs to process (can be specified multiple times). "
             "If not specified, reads from Tenants table in DB."
    )
    parser.add_argument(
        "--stage-only", action="store_true",
        help="Only run Extract + Transform + Load to staging tables. "
             "Skips stored procedure execution and data mart refresh."
    )
    parser.add_argument(
        "--skip", action="append", dest="skip_sources",
        help="Source types to skip (e.g. --skip sales --skip inventory)."
    )
    parser.add_argument(
        "--no-alerts", action="store_true",
        help="Disable email and Slack alerts."
    )
    parser.add_argument(
        "--no-truncate", action="store_true",
        help="Do not truncate staging tables before loading."
    )
    parser.add_argument(
        "--no-transform", action="store_true",
        help="Skip transform step (load raw extracted data)."
    )
    parser.add_argument(
        "--no-sp", action="store_true",
        help="Skip stored procedure execution."
    )
    parser.add_argument(
        "--no-datamart", action="store_true",
        help="Skip data mart refresh."
    )
    parser.add_argument(
        "--output", type=str,
        help="Write results to a JSON file."
    )
    parser.add_argument(
        "--log-level", type=str, default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set log level (default: from ETL_LOG_LEVEL env var)."
    )
    parser.add_argument(
        "--json-output", action="store_true",
        help="Output results as JSON."
    )

    scheduler_group = parser.add_argument_group("scheduler options")
    scheduler_group.add_argument(
        "--scheduler", action="store_true",
        help="Start the APScheduler background scheduler."
    )
    scheduler_group.add_argument(
        "--daily", type=int, metavar="HOUR",
        help="Schedule daily ETL run at this hour (0-23). "
             "Implies --scheduler. Use with --minute."
    )
    scheduler_group.add_argument(
        "--minute", type=int, default=0,
        help="Minute for scheduled run (default: 0)."
    )
    scheduler_group.add_argument(
        "--weekly", type=str, metavar="DAY",
        help="Schedule weekly ETL run on this day "
             "(monday, tuesday, ..., sunday). Implies --scheduler."
    )
    scheduler_group.add_argument(
        "--status", action="store_true",
        help="Show scheduler status and exit."
    )
    scheduler_group.add_argument(
        "--stop", action="store_true",
        help="Stop the scheduler."
    )

    return parser


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    """
    Entry point for the ETL orchestrator CLI.

    Returns:
        0 on success, 1 on failure.
    """
    global logger

    parser = _build_parser()
    args = parser.parse_args()

    # ---- Setup logging ----
    log_level = args.log_level or "INFO"
    setup_logging(
        log_level=log_level,
        log_file="etl.log",
        log_path="./logs",
        json_format=False,
    )
    logger = logging.getLogger("etl.main")
    logger.info("ETL Orchestrator starting | Args: %s", vars(args))

    # ---- Handle --status ----
    if args.status:
        status = get_scheduler_status()
        print(json.dumps(status, indent=2))
        return 0

    # ---- Handle --stop ----
    if args.stop:
        stop_scheduler()
        print("Scheduler stopped.")
        return 0

    # ---- Scheduler mode ----
    if args.scheduler or args.daily is not None or args.weekly is not None:
        return _run_scheduler_mode(args, log_level)

    # ---- One-shot run mode ----
    return _run_etl_mode(args)


def _run_scheduler_mode(args, log_level: str) -> int:
    """
    Start scheduler and register ETL jobs.
    """
    log = logging.getLogger("etl.main")
    log.info("Starting ETL scheduler...")

    sched = start_scheduler()

    if args.daily is not None:
        hour = args.daily
        minute = args.minute
        job_id = schedule_daily_etl(
            hour=hour,
            minute=minute,
            kwargs=_build_tenant_kwargs(args),
        )
        if job_id:
            print(f"Daily ETL scheduled at {hour:02d}:{minute:02d}")
        else:
            print("Failed to schedule daily ETL.", file=sys.stderr)
            return 1

    if args.weekly is not None:
        job_id = schedule_weekly_etl(
            day_of_week=args.weekly,
            hour=args.daily or 2,
            minute=args.minute,
            kwargs=_build_tenant_kwargs(args),
        )
        if job_id:
            print(f"Weekly ETL scheduled on {args.weekly}")
        else:
            print("Failed to schedule weekly ETL.", file=sys.stderr)
            return 1

    if not args.daily and not args.weekly:
        print("Scheduler started. No jobs scheduled. Use --daily or --weekly.")

    print(f"Scheduler running | PID={os.getpid()}")
    print("Press Ctrl+C to stop...")

    def signal_handler(sig, frame):
        print("\nShutting down scheduler...")
        stop_scheduler()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    while True:
        try:
            import time
            time.sleep(1)
        except KeyboardInterrupt:
            break

    stop_scheduler()
    return 0


def _run_etl_mode(args) -> int:
    """
    Run one-shot ETL for all/specified tenants.
    """
    log = logging.getLogger("etl.main")

    tenant_ids = args.tenants if args.tenants else None
    send_alerts = not args.no_alerts
    stage_only = args.stage_only
    truncate = not args.no_truncate
    skip_sources = args.skip_sources if args.skip_sources else None
    run_transform = not args.no_transform
    run_sp = not args.no_sp
    run_datamart = not args.no_datamart

    log.info(
        "Running ETL | tenants=%s | stage_only=%s | truncate=%s | "
        "send_alerts=%s | skip=%s",
        tenant_ids or "ALL",
        stage_only, truncate, send_alerts, skip_sources
    )

    try:
        result = run_all_tenants(
            tenant_ids=tenant_ids,
            stage_only=stage_only,
            send_alerts=send_alerts,
            truncate_before_load=truncate,
            skip_sources=skip_sources,
            run_transform=run_transform,
            run_sp_load=run_sp,
            run_datamart=run_datamart,
        )

        if args.json_output:
            print(json.dumps(result, indent=2, default=str))

        status = result["overall_status"]
        duration = result["duration_seconds"]
        tenants_total = result["total_tenants"]
        tenants_succeeded = result["tenants_succeeded"]
        tenants_failed = result["tenants_failed"]
        rows_loaded = result["total_rows_loaded"]

        log.info(
            "ETL completed | status=%s | %d/%d tenants | "
            "%d rows loaded | %ds",
            status,
            tenants_succeeded, tenants_total,
            rows_loaded, duration
        )

        print()
        print("=" * 60)
        print(f"  Status:  {status}")
        print(f"  Tenants: {tenants_succeeded}/{tenants_total} succeeded, "
              f"{tenants_failed} failed")
        print(f"  Rows:    {rows_loaded:,} loaded")
        print(f"  Duration: {duration}s")
        print("=" * 60)

        if args.output:
            with open(args.output, "w", encoding="utf-8") as f:
                json.dump(result, f, indent=2, default=str)
            log.info("Results written to: %s", args.output)

        if status == "FAILED":
            return 1
        return 0

    except Exception as ex:
        log.error("ETL orchestrator crashed: %s", ex, exc_info=True)
        print(f"ERROR: {ex}", file=sys.stderr)
        if args.output:
            json.dump({"error": str(ex), "status": "FAILED"}, f, indent=2)
        return 1


def _build_tenant_kwargs(args) -> dict:
    """Build kwargs dict for run_all_tenants from CLI args."""
    return {
        "stage_only": args.stage_only,
        "send_alerts": not args.no_alerts,
        "truncate_before_load": not args.no_truncate,
        "run_transform": not args.no_transform,
        "run_sp_load": not args.no_sp,
        "run_datamart": not args.no_datamart,
        "skip_sources": args.skip_sources,
    }


if __name__ == "__main__":
    sys.exit(main())
